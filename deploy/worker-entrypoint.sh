#!/bin/sh
set -eu

case "${WORKER_RUNTIME:-go}" in
  go)
    exec ./worker
    ;;
  rust)
    exec ./worker-rust
    ;;
  *)
    echo "invalid WORKER_RUNTIME='${WORKER_RUNTIME}'; expected 'go' or 'rust'" >&2
    exit 64
    ;;
esac
