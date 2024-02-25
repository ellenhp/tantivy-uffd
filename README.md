# Remote Directories for Tantivy

`tantivy-uffd` is a userfaultfd-based remote directory implementation for the tantivy search engine library. With this crate, you can run tantivy without needing a local copy of the search index, instead fetching pieces on-demand via HTTP range requests. This is similar in concept to [tantivy-wasm](https://github.com/phiresky/tantivy-wasm), but doesn't run in the browser. It's intended for situations where you may want to co-locate a search index with multiple tantivy serving instances, or situations where you'd like to opt for extreme cost saving measures and host a static index on object storage.

### Requirements

This crate should work on Linux >= 5.7. The endpoint you point `tantivy-uffd` at must support HTTP GET and HEAD requests, and must support range requests. I'm not actually sure what will happen if it doesn't support range requests. It's very possible that you'll get an OOM due to `tantivy-uffd` failing to notice and accidentally downloading the entire file. A user executing a program that uses `tantivy-uffd` should have read/write access to `/dev/userfaultfd`. If this is not the case, you will experience a permissions error when the directory is opened.

### Caveats

There's a lot of unsafe code in this crate, and if a remote directory is ever dropped you should assume that there will be undefined behavior. It should be possible to fix this but I haven't yet. Even if you use it exactly as intended, there's probably still a fair amount of UB lurking in the shadows, as this crate was originally written as a proof of concept for the [Airmail](https://github.com/ellenhp/airmail) project, without a priori knowledge that it would even work.

### Optimizations

Enabling the `quickwit` feature on tantivy will improve remote directory performance substantially, as will fetching documents in parallel with an appropriate number of `spawn_blocking` operations. A good starting point for CHUNK_SIZE can be found by estimating the bandwidth-delay product to your index host. I use 1-2MiB for Airmail. I would considering reducing this for an `fst` based index. If you search against a remote index with a large disjunction, it can be helpful to launch a search for each clause of the disjunction in parallel prior to searching for the main query. This can result in a lot of unnecessary traffic from your index, but userfaultfd page faults pause the entire thread and tantivy performs many dependent reads so launching a search for each clause of the disjunction starts a bunch of threads that will essentially perform readahead for the main thread. This can speed things up a lot.

I also would (counterintuitively) recommend configuring user-interface code to aggressively send queries as they're being typed, because it will warm the cache reducing perceived latency. The [Airmail demo](https://airmail.rs/) has a 100ms debounce on partial query requests (results are discarded) and a 500ms debounce on a final query whose results are displayed to the user. If you have the CPU cycles to spare this works very well.

### Alternatives

You can implement your own remote directory by fetching slices on-demand in `FileHandle::read_bytes`, but you'll end up needing to cache chunks and perform a lot of copies of those chunks when you recieve a `read_bytes` request spanning chunk boundaries. This ended up being an unacceptable performance cost for Airmail.

### License

Dual MIT/Apache 2.0, at your option. This crate contains some code from tantivy, namely `query_len.rs`. Copyright notices, the authors file and original MIT license have been preserved.
