1) Add tests for creating a new DB, util to drop db (or eventually a method) which executes after every test.
   1) Write and read large values; large keys (is there a max key size?) and values
2) Implement Delete method and add tests. Add some edge case tests around certain usage pattens like deleting everything.
3) Clean up TODO's and/or turn them in comments/roadmap
4) Implement compaction
   1) Implement End to End tests, write values, wait for compaction, read, etc.
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

TODO: What to call `KeyValue | Tombstone`? FileRecord? Only used in implementation context.

TODO: In DB file, is it better to filter out no-op delete writes, then serialize to bytes and index update command.
Split the stream in a way. Or inline logic into the pull function? Or rotate is simpler flatTap like?
