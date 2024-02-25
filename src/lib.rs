mod query_len;
mod uffd;
mod vec_writer;

use crate::{uffd::round_up_to_page, vec_writer::VecWriter};
use dashmap::DashMap;
use log::info;
use nix::sys::mman::{mmap, MapFlags, ProtFlags};
use std::{
    ops::{Deref, Range},
    path::Path,
    slice,
    sync::Arc,
};
use tantivy::{
    directory::{
        error::{DeleteError, OpenReadError, OpenWriteError},
        WatchHandle, WritePtr,
    },
    Directory,
};
use tantivy_common::{file_slice::FileHandle, HasLen, OwnedBytes, StableDeref};
use tokio::runtime::Runtime;
use uffd::UffdFile;
use userfaultfd::UffdBuilder;

thread_local! {
    pub(crate) static BLOCKING_HTTP_CLIENT: reqwest::blocking::Client = reqwest::blocking::Client::new();
}

#[derive(Clone)]
struct MmapArc {
    slice: &'static [u8],
}

impl Deref for MmapArc {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.slice
    }
}
unsafe impl StableDeref for MmapArc {}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CacheKey {
    base_url: String,
    path: String,
    chunk: usize,
}

#[derive(Debug, Clone)]
struct HttpFileHandle<const CHUNK_SIZE: usize> {
    owned_bytes: Arc<OwnedBytes>,
    _uffd_file: Option<Arc<UffdFile<CHUNK_SIZE>>>,
}

impl<const CHUNK_SIZE: usize> HttpFileHandle<CHUNK_SIZE> {
    pub(crate) fn new(runtime: Arc<Runtime>, file_size: usize, artifact_url: String) -> Self {
        let mmap_len = round_up_to_page(file_size, CHUNK_SIZE);
        let uffd = UffdBuilder::new()
            .close_on_exec(true)
            .user_mode_only(true)
            .create()
            .unwrap();

        let addr = unsafe {
            mmap(
                None,
                mmap_len.try_into().unwrap(),
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_PRIVATE | MapFlags::MAP_ANONYMOUS | MapFlags::MAP_NORESERVE,
                None::<std::os::fd::BorrowedFd>,
                0,
            )
            .expect("mmap")
        };

        let mmap_ptr = addr as usize;

        uffd.register(addr, mmap_len).unwrap();

        let uffd_file = Arc::new(UffdFile::new(
            Arc::new(uffd),
            runtime,
            mmap_ptr,
            artifact_url.clone(),
        ));
        {
            let uffd_file = uffd_file.clone();
            std::thread::spawn(move || {
                uffd_file.handle_faults();
            });
        }
        let owned_bytes = Arc::new(OwnedBytes::new(MmapArc {
            slice: unsafe { slice::from_raw_parts(mmap_ptr as *const u8, file_size) },
        }));

        Self {
            owned_bytes,
            _uffd_file: Some(uffd_file),
        }
    }
}

impl<const CHUNK_SIZE: usize> FileHandle for HttpFileHandle<CHUNK_SIZE> {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        Ok(self.owned_bytes.slice(range))
    }
}

impl<const CHUNK_SIZE: usize> HasLen for HttpFileHandle<CHUNK_SIZE> {
    fn len(&self) -> usize {
        self.owned_bytes.len()
    }
}

/// HTTP remote directory for tantivy. The directory is read-only, and is accessed on-demand by HTTP
/// range requests. The directory is backed by a large anonymous memory map, and pages are marked as
/// available to the kernel with MADV_FREE, meaning that the kernel can reclaim the memory if
/// needed. However, in situations of low memory pressure, previously fetched index pieces will
/// remain in memory for fast subsequent access. In practice, this means that several searches sent
/// in quick succession as a user is typing out a query will warm the cache for the final search,
/// making it feel faster.
#[derive(Debug, Clone)]
pub struct RemoteDirectory<const CHUNK_SIZE: usize> {
    base_url: String,
    file_handle_cache: Arc<DashMap<String, Arc<HttpFileHandle<CHUNK_SIZE>>>>,
    atomic_read_cache: Arc<DashMap<String, Vec<u8>>>,
    uffd_runtime: Arc<Runtime>,
}

impl<const CHUNK_SIZE: usize> RemoteDirectory<CHUNK_SIZE> {
    /// Create a new remote directory with the given base URL. The base URL can have a trailing
    /// slash or not, but a trailing slash will be appended if one is missing. For example, a
    /// request for `meta.json` on a directory with the base URL `http://localhost:8080` will result
    /// in a GET request to `http://localhost:8080/meta.json`.
    pub fn new(base_url: &str) -> Self {
        let rt = Runtime::new().unwrap();

        Self {
            base_url: base_url.to_string(),
            file_handle_cache: Arc::new(DashMap::new()),
            atomic_read_cache: Arc::new(DashMap::new()),
            uffd_runtime: Arc::new(rt),
        }
    }

    fn format_url(&self, path: &Path) -> String {
        if self.base_url.ends_with('/') {
            format!("{}{}", self.base_url, path.display())
        } else {
            format!("{}/{}", self.base_url, path.display())
        }
    }
}

impl<const CHUNK_SIZE: usize> Directory for RemoteDirectory<CHUNK_SIZE> {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        let url = self.format_url(path);
        {
            if let Some(file_handle) = self.file_handle_cache.get(&url) {
                return Ok(file_handle.clone());
            }
        }
        let file_len = query_len::len(&url);
        let len = round_up_to_page(file_len, CHUNK_SIZE);

        if len == 0 {
            return Ok(Arc::new(HttpFileHandle::<CHUNK_SIZE> {
                owned_bytes: Arc::new(OwnedBytes::new(MmapArc { slice: &[] })),
                _uffd_file: None,
            }));
        }

        let file_handle = Arc::new(HttpFileHandle::<CHUNK_SIZE>::new(
            self.uffd_runtime.clone(),
            file_len,
            url.clone(),
        ));
        self.file_handle_cache.insert(url, file_handle.clone());

        Ok(file_handle)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        if path == Path::new(".tantivy-meta.lock") {
            return Ok(());
        }

        Err(DeleteError::IoError {
            io_error: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Delete not supported",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if path == Path::new(".tantivy-meta.lock") {
            return Ok(true);
        }
        Ok(query_len::len(&self.format_url(path)) > 0)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        if path == Path::new(".tantivy-meta.lock") {
            return Ok(WritePtr::new(Box::new(VecWriter::new(path.to_path_buf()))));
        }
        dbg!(path);
        Err(OpenWriteError::IoError {
            io_error: Arc::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Write not supported",
            )),
            filepath: path.to_path_buf(),
        })
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        let url = self.format_url(path);
        if let Some(bytes) = self.atomic_read_cache.get(&url) {
            return Ok(bytes.clone());
        }

        info!("Fetching {} in atomic read.", url);
        let response = BLOCKING_HTTP_CLIENT.with(|client| client.get(&url).send());
        let response = if let Err(_e) = response {
            return Err(OpenReadError::IoError {
                io_error: Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Fetch failed for atomic read.",
                )),
                filepath: path.to_path_buf(),
            });
        } else {
            response.unwrap()
        };
        let bytes = response.bytes().unwrap();

        let bytes = bytes.to_vec();
        self.atomic_read_cache.insert(url, bytes.clone());
        Ok(bytes)
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Write not supported",
        ))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(
        &self,
        _watch_callback: tantivy::directory::WatchCallback,
    ) -> tantivy::Result<tantivy::directory::WatchHandle> {
        Ok(WatchHandle::empty())
    }
}

#[cfg(test)]
pub(crate) mod test {

    use std::{path::PathBuf, str::FromStr, sync::OnceLock};

    use tantivy::{directory::ManagedDirectory, doc, schema::Field, Directory, Index};
    use tiny_http::{Header, Method, Response, Server};

    pub(crate) static TEST_SERVER_BASE_URL: OnceLock<String> = OnceLock::new();

    pub(crate) fn test_schema_name() -> Field {
        test_schema().get_field("name").unwrap()
    }

    pub(crate) fn test_schema_doc() -> Field {
        test_schema().get_field("doc").unwrap()
    }

    pub(crate) fn test_schema() -> tantivy::schema::Schema {
        let mut schema_builder = tantivy::schema::Schema::builder();
        schema_builder.add_text_field("name", tantivy::schema::TEXT | tantivy::schema::STORED);
        schema_builder.add_text_field("doc", tantivy::schema::TEXT | tantivy::schema::STORED);
        schema_builder.build()
    }

    fn init_test_index_no_remote() -> ManagedDirectory {
        let schema = test_schema();
        let index = Index::create_in_ram(schema);
        let index = std::thread::spawn(move || {
            let mut writer = index.writer(15_000_000).unwrap();
            writer
                .add_document(doc!(
                    test_schema_name() => "LICENSE_MIT",
                    test_schema_doc() => include_str!("../LICENSE_MIT"),
                ))
                .unwrap();
            writer
                .add_document(doc!(
                    test_schema_name() => "LICENSE_APACHE",
                    test_schema_doc() => include_str!("../LICENSE_APACHE"),
                ))
                .unwrap();
            writer.commit().unwrap();
            drop(writer);
            let ids = index.searchable_segment_ids().unwrap();
            let writer = index.writer(15_000_000).unwrap();

            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let mut writer = writer;
                writer.merge(&ids).await.unwrap()
            });

            index
        })
        .join()
        .unwrap();
        let dir = index.directory().clone();
        drop(index);

        for path in dir.list_managed_files() {
            if path.ends_with("meta.json") {
                continue;
            }
            dir.validate_checksum(&path).unwrap();
        }

        dir
    }

    pub(crate) fn test_index() -> Index {
        // Low chunk size to test multi-chunk reads without needing a big index.
        let http_directory =
            super::RemoteDirectory::<8192>::new(&TEST_SERVER_BASE_URL.get().unwrap());
        Index::open(http_directory).unwrap()
    }

    fn run_test_server() {
        let test_index = init_test_index_no_remote();

        let server = Server::http("127.0.0.1:0").unwrap();

        std::thread::spawn(move || {
            TEST_SERVER_BASE_URL.get_or_init(|| format!("http://{}", server.server_addr()));
            for req in server.incoming_requests() {
                let path = req.url().trim_start_matches('/');
                if req.method() == &Method::Get {
                    let data = if let Some(range_header) = req
                        .headers()
                        .iter()
                        .find(|h| h.field.as_str().to_ascii_lowercase() == "range")
                    {
                        let data = test_index
                            .atomic_read(&PathBuf::from_str(path).unwrap())
                            .unwrap();

                        let range = {
                            let range_str = range_header.value.to_string();
                            let range_str = range_str.split('=').last().unwrap();
                            let range = range_str.split('-').collect::<Vec<&str>>();
                            let start = range[0].parse::<usize>().unwrap();
                            let end = (1 + range[1].parse::<usize>().unwrap()).min(data.len());
                            start..end
                        };
                        data[range].to_vec()
                    } else {
                        test_index
                            .atomic_read(&PathBuf::from_str(path).unwrap())
                            .unwrap()
                    };
                    let response = Response::from_data(data);
                    req.respond(response).unwrap();
                } else if req.method() == &Method::Head {
                    let len = test_index
                        .atomic_read(&PathBuf::from_str(path).unwrap())
                        .unwrap()
                        .len();
                    let mut response = Response::from_string("".to_string());
                    response.add_header(
                        Header::from_bytes(&b"Content-Length"[..], len.to_string()).unwrap(),
                    );
                    req.respond(response).unwrap();
                }
            }
        });
    }

    #[ctor::ctor]
    fn ctor_init() {
        run_test_server();
    }

    #[test]
    fn test_has_meta_json() {
        let http_directory =
            super::RemoteDirectory::<8192>::new(&TEST_SERVER_BASE_URL.get().unwrap());
        assert!(
            http_directory
                .atomic_read(std::path::Path::new("meta.json"))
                .unwrap()
                .len()
                > 0
        );
    }

    #[test]
    fn test_has_docs() {
        let reader = test_index().reader().unwrap();
        assert_eq!(reader.searcher().num_docs(), 2);
    }

    #[test]
    fn search_docs() {
        let index = test_index();
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();
        let query_parser = tantivy::query::QueryParser::for_index(&index, vec![test_schema_name()]);
        let query = query_parser.parse_query("LICENSE_MIT").unwrap();
        let top_docs = searcher
            .search(&query, &tantivy::collector::TopDocs::with_limit(10))
            .unwrap();
        assert_eq!(top_docs.len(), 1);
    }
}
