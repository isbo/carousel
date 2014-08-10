### Carousel

Carousel is a distributed messaging library with atleast-once delivery semantics. 
It is designed to be highly available and durable. It does not guarantee strict FIFO ordering of messages. It is ideal for implementing task "pools" where tasks are idempotent and strict queuing semantics are not required. 

### Installation

Carousel is implemented as a library on top of the Riak data store. 
Install Riak 2.0, setup the cluster, and then run:

`riak-admin bucket-type create carousel '{"props":{"datatype":"set"}}'`

`riak-admin bucket-type activate carousel`

### Usage

Each carousel has a name and is internally partitioned into multiple shards for performance. Each item that is added to the carousel has a unique key and a payload. Upon `add()`, the item is stored in a shard. Items can be retrieved via
`get()` in no particular order (although for now all items in a shard are 
returned in order of the insertion timestamp). Once the item is processed, it 
can be removed via `remove()`. 

### Internals

A Carousel can be thought of as a set of items. We use Riak's set CRDT to store them. Riak implementation is an Observed Removed (OR) CRDT set although we can technically get by with a 2P set.