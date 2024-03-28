1) Add tests for creating a new DB, util to drop db (or eventually a method) which executes after every test.
   1) Create a DB and close it. Check directory effects.
   2) (LATER) Can reopen DB and close it.
   3) Create a DB, read empty, write, then read.
   4) (LATER) Create a DB, write, close it, reopen DB, read.
   5) Create a DB, write a bunch of random keys, then read them all.
   6) (LATER) Same as, but multiple reopens with more writes. "Rolling reopens with new writes and reads".
   7) Create a DB, write keys with overwrites, and read.
   8) Test crash during read, confirm Checksum works
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