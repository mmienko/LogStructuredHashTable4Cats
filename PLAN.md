1) Add tests for creating a new DB, util to drop db (or eventually a method) which executes after every test.
   1) Create a DB and close it. Check directory effects.
   2) (LATER) Can reopen DB and close it.
   3) Create a DB, read empty, write, then read.
   4) (LATER) Create a DB, write, close it, reopen DB, read.
   5) Create a DB, write a bunch of random keys, then read them all.
   6) (LATER) Same as above, but multiple reopens with more writes. "Rolling reopens with new writes and reads".
   7) Create a DB, write keys with overwrites, and read.
   8) Test crash during read, confirm Checksum works
   9) (LATER) concurrent closure with in-progress writes, cancels writes
   10) Write and read large values; large keys (is there a max key size?) and values
   11) Errors bubble up; write to a file and then move it behind the scenes to create some error
   12) Test partially written values, mimicking a crash
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