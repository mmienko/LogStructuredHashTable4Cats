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