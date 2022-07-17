# codingtest


#BASIC STRATEGY
****************

We find number of cpus in the system and start as many worker threads and give
each of them the rx end of a channel where they will receive input records to
process. The main thread creates as many mpsc channels as worker threads count.

The main thread reads inputs records from a file  and deserializes then using
serde/csv and based on client id distributes the work to one of the worker
threads by writing the record to write end of the corresponding channel.  Note
that mapping from a client to a worker thread is fixed (just a simple modulo).
Since the same thread manipulates the same set of clients, we don't need
mutexes/locks etc. Each thread maintains a hashmap indexed by client id which
contains the client account information. As transactions are processed account
information is updated and finally it writes the account information output to
stdout.


#ERROR HANDLING
****************

For error handling we mostly panic.  For e.g. if the input data is not in
expected format we don't try to skip or fix them, rather panic and exit.  Even
if basic infra fails like say main thread is not able to join with a spawned
worker threads, the whole program just panics.

Some cases we handle, for e.g. we ignore case for transaction type, or check
before handling "resolve" or "chargeback" record, that the transaction it is
referring to is indeed has been disputed previously.

#TESTING
**********

Tested mosly with unit tests to check the basic business logic.  Also
distributing work to various workers is also tested (just 2 in test), but at a
much more basic level.  We just check that each worker thread handles the
clients it is supposed to.  Possibly we can do better here.

No integration tests have been added
