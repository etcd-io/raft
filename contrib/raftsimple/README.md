# raftsimple

<!-- `raftsimple` is a simplified, educational version of etcd's `raftexample`. While `raftexample` demonstrates production-grade integration with `etcd/raft`, `raftsimple` focuses on minimalism, providing a self-contained implementation with a custom HTTP transport and a basic file-based Write-Ahead Log (WAL). -->

This project is designed for developers who want to understand the core mechanics of the Raft algorithm and the `Ready()` event loop without the complexity of the full etcd server ecosystem.

## Getting started

### Building raftsimple

Build the binary:

```bash
go build -o raftsimple
```

### Running single-node raftsimple

To start a single-node Raft cluster, run:

```bash
./raftsimple --id 1 --cluster http://127.0.0.1:9021 --port 9121
```

This starts a node with ID 1, listening for Raft messages on `127.0.0.1:9021`, and exposing a key-value API on port `9121`.

You can now interact with the key-value store:

```bash
# Propose a value
curl -L http://127.0.0.1:9121/foo -XPUT -d bar

# Retrieve the value
curl -L http://127.0.0.1:9121/foo
```

### Running a local cluster

For a more realistic distributed setup, use [goreman](https://github.com/mattn/goreman) to run a 3-node cluster:

```bash
goreman start
```

This uses the included `Procfile` to start three independent `raftsimple` processes on different ports. Each node can be communicated with via its respective key-value API at ports `9121`, `9122`, and `9123`.

### Fault Tolerance

Raft's key feature is its ability to withstand node failures. You can simulate a node crash with `goreman` (it uses Procfile file):

```bash
# Stop Node 2
goreman run stop raft2
```

The cluster will still operate with Node 1 and Node 3. Propose a new value on Node 1:

```bash
curl -L http://127.0.0.1:9121/foo -XPUT -d baz
```

Now restart Node 2:

```bash
goreman run restart raft2
```

Node 2 will automatically recover its state from its local WAL and snapshot, then catch up on any missed data from the leader. You can verify it has recovered:

```bash
curl -L http://127.0.0.1:9122/foo
```

### Dynamic Reconfiguration

`raftsimple` also supports membership changes via the KV API. To add a new node (Node 4) to the cluster:

```bash
# Register Node 4 with the cluster, providing its URL in the request body
curl -L http://127.0.0.1:9121/4 -XPOST -d http://127.0.0.1:9024

# Start Node 4 in a separate terminal with the --join flag
./raftsimple --id 4 --cluster http://127.0.0.1:9021,http://127.0.0.1:9022,http://127.0.0.1:9023,http://127.0.0.1:9024 --port 9124 --join
```

Similarly, to remove a node from the cluster:

```bash
curl -L http://127.0.0.1:9121/4 -XDELETE
```

## Design

The `raftsimple` architecture is divided into three main components:

1.  **KV Store (`kvstore.go`):** The application-level state machine. It manages the actual key-value data and provides methods for proposing changes to the Raft log and applying committed entries.
2.  **REST API (`httpapi.go`):** The user-facing interface. It handles incoming HTTP requests and converts them into proposals for the Raft node.
3.  **Raft Node (`raft.go`):** The core consensus engine. It manages the `etcd/raft` state machine, handles the `Ready()` loop, and coordinates with the transport and storage layers.

## Project Philosophy: Simplicity First

`raftsimple` is designed as an educational tool, not a production-grade database. To keep the code as readable and linear as possible, we have made deliberate design choices that prioritize **simplicity over robustness**:

*   **Synchronous Storage:** Unlike production systems that use complex asynchronous I/O and batching, `raftsimple` writes to disk synchronously within the main Raft loop.
*   **Minimalist WAL:** The Write-Ahead Log is a simple append-only file. It does not include checksums, CRCs, or automatic truncation after snapshots.
*   **Simple Transport:** The inter-node communication uses standard Go `net/http` for simplicity, rather than the more complex but efficient `rafthttp` package used in `etcd`.
*   **Direct File I/O:** Snapshots and HardState are overwritten directly on disk.

These choices make the codebase significantly easier to audit and learn from, though they mean the project should not be used for storing critical data.
