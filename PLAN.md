1) Add tests for creating a new DB, util to drop db (or eventually a method) which executes after every test.
   1) Write and read large values; large keys (is there a max key size?) and values
2) Implement Delete method and add tests. Add some edge case tests around certain usage pattens like deleting everything.
3) Implement rotating files
4) Implement compaction
   1) Test basics of compaction, w/o worrying about failed compaction processes
   2) Review naming
   3) Review error translation
   4) Review test organization and method
   5) Review TODO's and compiler warnings
   6) Implement End to End tests, write values, wait for compaction, read, etc.
5) Write TLA+ specs
6) Refine tests around crashes
   1) Crash during rotation
   2) Crash during compaction
7) Performance tests?
   1) Read/Write Latency
   2) Startup time, compaction should help loading indexes
8) Implement ReadCursor resource object pool
9) Implement TTL
10) Does it make sense to use different Checksums depending on the length of data? Is there different performance? This could be inlined as a note.
11) Use scala property based testing for PutCodec and other parts of codebase?
12) Implement a CLI app to interface with DB
13) How could a secondary index be implemented?

There is a _DB_ which creates a _LSHT_ which then creates a _hash table_ (_HT_), loads data from disk into it, starts a single
threaded process to append writes to an _active data file_ (which is mutable append only log) with _file rotation_, and starts a _compaction process_
over the older _inactive data files_ (which are immutable) and _compacted files_. The latter is a pair of _hint_ and _values_ files.

Loading from disk reads the _hint_ file, _inactive data files_, and _active data file_.

s/_hint_/_keys_ or s/_hint_/_keys_with_value_references_? 

A _key_ _value_ pair is called an _KeyValue_. Mhh, entry sounds overused.

Deletions create _tombstones_, special entries in _data files_ that mark a key for pruning during _compaction_, so
that the key doesn't enter the _compacted files_. Thus is won't be reloaded in case _DB_ stops and crashes.

The _HT_ is a list of tuples, _key_ and _EntryFileReference_. s/_EntryFileReference_/_FileReference_ OR s/_EntryFileReference_/_KeyValueEntryFileReference_

TODO: What to call `KeyValue | Tombstone`? FileRecord? Only used in implementation context.

_HintFileReference_ ...

TODO: In DB file, is it better to filter out no-op delete writes, then serialize to bytes and index update command.
Split the stream in a way. Or inline logic into the pull function? Or rotate is simpler flatTap like?

TODO: Def generate a new active data file on each startup. Compaction should gracefully handle empty files. Assuming
that active data file has strictly higher number than older, clock drift can undermine that but we can document and/or
throw error. New file code could also enforce that new file ts is one greater than last, which it should have context about.  

Under the hood, the code uses _codecs_ to _encode_ and _decode_ _KeyValueEntries_ to bytes.

Each concept should have representation in code. Stop abusing `|` type. 