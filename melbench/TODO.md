To keep this first cut small and usable, I did not add:

* connection re-creation if a socket dies (right now it counts the error and drops that connection)
* pipelining / multiple in-flight per connection (`wrk` can do this, we can add later)
* a more precise histogram (this is log2-bucketed, good enough to find knees/cliffs)
