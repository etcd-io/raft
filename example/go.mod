module go.etcd.io/raft/v3/example

go 1.21

require (
	github.com/sirupsen/logrus v1.9.3
	go.etcd.io/raft/v3 v3.0.0
)

replace go.etcd.io/raft/v3 => ../

require (
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	golang.org/x/sys v0.15.0 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
)
