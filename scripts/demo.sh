#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

BIN="${CARGO_TARGET_DIR:-target}/debug"
ORIGIN_ADDR=127.0.0.1:18080
CTRL_HTTP=127.0.0.1:18090
CTRL_DNS=127.0.0.1:18053

need() {
  if [[ ! -x "$1" ]]; then
    echo "missing executable: $1" >&2
    exit 1
  fi
}

alive() {
  local pid=$1 name=$2
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "process died: $name pid=$pid" >&2
    exit 1
  fi
}

wait_tcp() {
  local hostport=$1
  local host=${hostport%:*}
  local port=${hostport##*:}
  for _ in $(seq 1 100); do
    if nc -z "$host" "$port" 2>/dev/null; then
      return 0
    fi
    # bash /dev/tcp fallback
    if (echo >/dev/tcp/"$host"/"$port") >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "timeout waiting for $hostport" >&2
  exit 1
}

wait_locate() {
  for _ in $(seq 1 80); do
    if curl -sf "http://${CTRL_HTTP}/locate?path=/warmup" >/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  echo "timeout waiting for control-plane /locate (heartbeats not registered)" >&2
  exit 1
}

echo "building workspace binaries..."
cargo build --workspace --bins

need "$BIN/origin-mock"
need "$BIN/control-plane"
need "$BIN/edge-node"
need "$BIN/traffic-gen"

export ORIGIN="$ORIGIN_ADDR"
export CONTROL_PLANE="http://${CTRL_HTTP}"

"$BIN/origin-mock" "$ORIGIN_ADDR" 8192 0 &
ORIGIN_PID=$!
echo "origin-mock pid=$ORIGIN_PID"
alive "$ORIGIN_PID" origin-mock
wait_tcp "$ORIGIN_ADDR"

"$BIN/control-plane" "$CTRL_HTTP" "$CTRL_DNS" &
CTRL_PID=$!
echo "control-plane pid=$CTRL_PID"
alive "$CTRL_PID" control-plane
wait_tcp "$CTRL_HTTP"

BIND=127.0.0.1:18081 NODE_ID=edge-a PRICE=1.2 RTT_MS=8 ORIGIN="$ORIGIN" CONTROL_PLANE="$CONTROL_PLANE" CACHE_BYTES=1048576 "$BIN/edge-node" &
A_PID=$!
echo "edge-a pid=$A_PID"

BIND=127.0.0.1:18082 NODE_ID=edge-b PRICE=0.8 RTT_MS=12 ORIGIN="$ORIGIN" CONTROL_PLANE="$CONTROL_PLANE" CACHE_BYTES=1048576 "$BIN/edge-node" &
B_PID=$!
echo "edge-b pid=$B_PID"

BIND=127.0.0.1:18083 NODE_ID=edge-c PRICE=1.5 RTT_MS=4 ORIGIN="$ORIGIN" CONTROL_PLANE="$CONTROL_PLANE" CACHE_BYTES=1048576 "$BIN/edge-node" &
C_PID=$!
echo "edge-c pid=$C_PID"

cleanup() {
  kill $ORIGIN_PID $CTRL_PID $A_PID $B_PID $C_PID 2>/dev/null || true
}
trap cleanup EXIT

alive "$A_PID" edge-a
alive "$B_PID" edge-b
alive "$C_PID" edge-c

wait_tcp 127.0.0.1:18081
wait_tcp 127.0.0.1:18082
wait_tcp 127.0.0.1:18083
wait_locate

echo "== nodes =="
curl -s "http://${CTRL_HTTP}/nodes"
echo

echo "== traffic =="
LOCATE="http://${CTRL_HTTP}" ORIGIN="$ORIGIN_ADDR" REQUESTS=80 KEYS=12 SIZE=4096 "$BIN/traffic-gen"
