# Log-Structured Hash Table for Scala Cats

A Log-Structured Hash Table implemented in functional manor. The core concepts are based off of 
[Bitcask](https://github.com/basho/bitcask/tree/develop), the DB engine for [Riak](https://riak.com/index.html).
The inspiration to write a DB engine from scratch was thanks to
[Designing Data-Intensive Applications](https://www.oreilly.com/library/view/designing-data-intensive-applications/9781491903063/)
by [Martin Kleppmann](https://martin.kleppmann.com/), as an attempt to learn in depth how a "simple" database works.

## Core Concepts

The _Database_ creates a _Log-Structured Hash Table_, a _hash table_ backed by an append
only log. On start, the _database_ loads data from disk into the _hash table_, starts a
single-threaded-like process (Cats Effect fiber) to append writes to an _active data file_
(the mutable append only log). The single threaded nature of writes remove concurrency
control mechanism. The log undergoes _file rotation_ to be picked up by a _compaction
process_. These _inactive data files_ (which are immutable) are merged into a single
file, with duplicates and _tombstones_ (keys marked for removal) pruned. The resulting
file are called _compacted files_. The merged _inactive data files_ are deleted to free
up disk space. _Compacted files_ are 2 files, a _keys_ file and _values_ file. The _keys_
file contains a pointer to value in the _values_ file. By having keys and values split,
on _Database_ start, it is quicker to read just the keys; values always stay on disk to
save memory. The complete process of loading from disk reads the _keys_ file and all
_inactive data files_ (the previous _active data file_ is now inactive). All writes to
files add a checksum to the serialized bytes. This allows the _Database_ to detect if it
crashed during a write. Internally, a suite of _codecs_ are used to serialize data.

## Outstanding issues
While the Database engine is fully functional, there are many optimizations left.
- Testing
  - Writing and Reading large values; there is no max key or value size defined
  - Pathological query patterns, such as puts followed by deleting everything
  - Complete some TODO's in test files, like
    - testing Tombstones in `DataFileDecoderTest`
    - CRC detection in `CompactedKeyCodecTest`
    - test for delete commands in `DatabaseCorruptionTest`
  - Read heavy vs. write heavy workloads
  - "End-to-End" tests like: write data, wait for compaction, restart db, read data
  - Edge cases around crashing during compaction
  - Explore property based testing
- Performance measurements/Improvements
  - Read and Write latency performance tests
  - Read and Write throughput performance tests
  - startup times
  - Confirm that index loading during startup is efficient. Each data file is read;
    make sure that not all values are in memory at the same time as the `fold` command
    on the `Stream` is after `compile`. Does it need to be before `compile` to be
    executed in streaming fashion?
  - performance during compaction
  - Explore what the `limit` in `Queue.fromQueueUnterminated` should be. Currently
    set to 1 as the write commands need to be serially executed, but Stream logic
    may already handle that.
  - Explore if startup times could be improved through parallel loading of compaction
    files. `fs2.Files[F].readRange` could be used. Compacted files are safe to read
    in parallel b/c keys are unique.
  - Optimize reading from files; currently a new file handle is created per read, but
    there could be an object pool to recycle.
  - Explore `immutable.VectorMap` vs `mutable.LinkedListHashMap` in Compaction process.
- Explore adding a timestamp field to Key Values pairs for informational purposes &
- Explore implementing a TTL
- Explore different checksum algorithms as they have different properties based on
  length of data. Could switch on algorithm depending on length or metadata settings.
- Reduce boiler plate in codec code. Is it possible to capture similar behavior in
  a trait/method. For state transitions like in DataFileDecoder, is it simpler to use
  less recursion?
- Explore how to implement a secondary index
- Rotate files based on more sophisticated properties of data files like how many keys
  are overwritten. The more rewrites, the more the files can be compacted, thus saving
  space and startup times.
- The current load index process loads from oldest to newest, so values are kept until they are deleted. It may
  be more advantageous to instead load from more recent to oldest. Then keys and values are kept
  with certainty and the index updates are purely additive between files. Since data files can
  only be read from beginning of file (the log is like a singly linked list), then within the
  processing of a file, there may be deletes.
- Explore how to handle cancellation, specifically around writes. Ideally, once a write command
  is in the Write Queue, then it is completed or failed, i.e. it is always completed and there
  is no possibility of it being abandoned. The calling fiber waits on a Deferred.
- Terminate Write Command Queue with None for clean and simple close; there could be
  concurrent writes while Database is closing.
- Should code protect against resource leaks via the isClosed Ref?
- Remove Console context bound on F, instead use a logger or Write monad.
- Resolve conflicts when new data files are created and file already exists (unlikely edge case).
- Resolve conflict if new data file is created with an earlier timestamp! Unlikely, but if
  database rotates quickly and time jumps back (due to system clock adjustment). Database
  simply needs to enforce an auto incrementing id, thus if needs to hold state of active data file.
- `CompactionFilesUtil.readKeyValueEntries` can have clearer error handling during `readValuesAndCombine`.
  However, since it's only currently used for tests, then it's not a high priority and errors can be
  thrown in the F and Stream.
- Create a debugging utility package/cli-app that can be used to manually inspect and alter DB files.
  This would reuse the Codecs and Utilities packages as well modularize any other reading logic that
  DB uses, so that code can be shared between modules.
- If engine is a module that could be imported, figure out how to hide internal classes like
  Compaction, so that the api is clean.
- Should there be a critical section around compaction to ensure only process executes at a time?
  Probably already handled by Stream properties.
- In places like `DataFileDecoder`, Scala 3's or type, `|`, is used. It's not clear if this a good
  use of this feature and if instead an ADT should be used. For example, `ParsedKeyValue`
  might be better using an ADT like `LoadCommand` with `Put(kv: KeyValue, offset: Offset)` and
  `Delete(key: Tombstone)` as subtypes.
- Fine tune Codec decoders/byte-parsers to have more fine grained error handling. Typically, they
  call `s.pull.unconsN(HeaderSize)` on the Stream and potentially miss incomplete headers. The
  DB code still ends up being correct, loading a incomplete header b/c DB crashed, gets skipped.
  However, that could be reported in error channel for informational logging. Then file could be cleaned up!
- Implement error handling around failed compaction process. Create "staged" compaction files, upon finish
  move files to "main" area ("unstage" files) then delete inactive data files and compacted files.
  The timestamp will act as a signal to restarts. If DB crashes in staging, then process staging is
  cleared and compaction is repeated. If at least one of the compacted files is "unstaged" (moved)
  then upon restart the other can be moved and old data files deleted. If DB crashes during deletion,
  then upon restart, data and compacted files with timestamp earlier than latest compacted files,
  can be deleted.

