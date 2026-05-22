#!/usr/bin/env bash
#
# Generate all etcd protobuf bindings.
# Run from repository root directory named etcd.
#
set -e
shopt -s globstar

if ! [[ "$0" =~ scripts/genproto.sh ]]; then
  echo "must be run from repository root"
  exit 255
fi

source ./scripts/test_lib.sh

if [[ $(protoc --version | cut -f2 -d' ') != "3.20.3" ]]; then
  echo "could not find protoc 3.20.3, is it installed + in PATH?"
  exit 255
fi

GOGEN_BIN=$(tool_get_bin google.golang.org/protobuf/cmd/protoc-gen-go)
GOGOPROTO_ROOT="$(tool_pkg_dir github.com/gogo/protobuf/proto)/.."

echo
echo "Resolved binary and packages versions:"
echo "  - protoc-gen-go:           ${GOGEN_BIN}"
echo "  - gogoproto-root:          ${GOGOPROTO_ROOT}"
GOGOPROTO_PATH="${GOGOPROTO_ROOT}:${GOGOPROTO_ROOT}/protobuf"

# directories containing protos to be built
DIRS="./raftpb"

log_callout -e "\\nRunning protoc-gen-go proto generation..."

for dir in ${DIRS}; do
  pushd "${dir}"
    protoc --go_out=. -I=".:${GOGOPROTO_PATH}:${RAFT_ROOT_DIR}/..:${RAFT_ROOT_DIR}" \
    --plugin=protoc-gen-go="${GOGEN_BIN}" \
    --go_opt=paths=source_relative ./**/*.proto

    rm -f ./**/*.bak
    gofmt -s -w ./**/*.pb.go
    run_go_tool "golang.org/x/tools/cmd/goimports" -w ./**/*.pb.go
  popd
done

log_success -e "\\n./genproto SUCCESS"
