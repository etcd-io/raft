# raftexample

raftexample is an example usage of etcd's [raft library](https://github.com/etcd-io/raft). It provides a simple REST API for a key-value store cluster backed by the [Raft][raft] consensus algorithm.

[raft]: http://raftconsensus.github.io/

## Getting Started

### Building raftexample

To Build `example`:

```sh
git clone git@github.com:etcd-io/raft.git
cd ./raft/example
go mod tidy
go build -o bin/raftexample main.go
```

### Running single node raftexample

First start a single-member cluster of raftexample:

```sh
./bin/raftexample --id 1 --cluster http://127.0.0.1:12379 --port 12379
```

Each raftexample process maintains a single raft instance and a key-value server.
The process's list of comma separated peers (--cluster), its raft ID index into the peer list (--id), and http key-value server port (--port) are passed through the command line.

Next, store a value ("hello") to a key ("my-key"):

```
curl -L http://127.0.0.1:12379/my-key -XPUT -d hello
```

Finally, retrieve the stored key:

```
curl -L http://127.0.0.1:12379/my-key
```

### Dynamic cluster reconfiguration

Nodes can be added to or removed from a running cluster using requests to the REST API.

For example, suppose we have a 3-node cluster that was started with the commands:

```
./bin/raftexample --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12379
./bin/raftexample --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22379
./bin/raftexample --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32379
```


A fourth node with ID 4 can be added by issuing a POST:
```sh
./bin/raftexample --id 4 --cluster http://127.0.0.1:42379 --port 42379

curl -L http://127.0.0.1:12379/4 -XPOST -d http://127.0.0.1:42379
```

We can remove a node using a DELETE request:
```sh
curl -L http://127.0.0.1:12379/3 -XDELETE
```

Node 3 should shut itself down once the cluster has processed this request.
