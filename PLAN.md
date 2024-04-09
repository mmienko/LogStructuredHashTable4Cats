1) Add tests for creating a new DB, util to drop db (or eventually a method) which executes after every test.
   1) concurrent closure with in-progress writes, cancels writes 
   2) Write and read large values; large keys (is there a max key size?) and values
2) Implement Delete method and add tests. Add some edge case tests around certain usage pattens like deleting everything.
3) Implement rotating files
4) Implement compaction
5) Write TLA+ specs
6) Refine tests around crashes
   1) Crash during rotation
   2) Crash during compaction
7) Performance tests?
   1) Read/Write Latency
   2) Startup time, compaction should help loading indexes
8) Implement ReadCursor resource object pool
9) Implement TTL
10) Does it make sense to use different Checksums depending on the length of data? Is there different performance?
11) Use scala property based testing for PutCodec and other parts of codebase?