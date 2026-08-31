# Skynet

A lab CDN: **cache-aware, cost-driven edge routing** in Rust.

Clients ask a control plane which edge should serve an object. Each edge is an HTTP data plane with an in-memory byte-capped LRU, single-flight origin fetches, and Prometheus metrics. Steering does **not** blindly pick the cheapest PoP — it hashes each key to a replica set, then picks the cheapest *viable* replica, with a bonus if that node already has the object.

```text
Traffic gen  ->  HTTP /locate (per object)  ->  Edge A/B/C
                      ^                              |
                      | heartbeats                   | miss + single-flight
                      +------------------------------+ --> origin-mock
```

UDP DNS for `cdn.local` is **host-level** (same chooser, key `cdn.local`). Per-object routing in the demo is HTTP `/locate`. TLS is deferred.

## Why this exists

Portfolio / systems-interview signal: bounded memory, thundering-herd coalescing, independent per-node caches, an explicit score function, and tests plus a demo that prove the algorithm.

## Run tests

```bash
cargo test --workspace
```

## Demo

The script builds **workspace member binaries** (`cargo build --workspace --bins`) into `target/debug/` (or `$CARGO_TARGET_DIR/debug`). That is a debug lab run, not a release profile.

```bash
./scripts/demo.sh
```

This starts origin `:18080`, control plane HTTP `:18090` (lab DNS `:18053`, not 5353 — that port is mDNS), three edges `:18081-18083`, waits until `/locate` succeeds, then a Zipf load generator.

Optional release binaries: `cargo build --release --workspace --bins` → `target/release/`.

## Proof

Captured from `./scripts/demo.sh` (80 Zipf requests, 12 keys, 4096-byte objects):

```text
proof
  requests=80 ok=80 fail=0
  p50_ms=0.566 p99_ms=1.421
  cache_hits=69 cache_misses=11 hit_ratio=0.8625
  origin_fetches_edges=11 origin_hits_origin=11
  bytes_served=327680 cost_units=335872.0000 cost_per_byte=1.025000
  elapsed_ms=56
  per_node: edge-c hits=0 origin=0 bytes=0 cost=0.0000
  per_node: edge-a hits=40 origin=5 bytes=184320 cost=221184.0000
  per_node: edge-b hits=29 origin=6 bytes=143360 cost=114688.0000
  steered: edge-a=45
  steered: edge-b=35
```

`cost_units` is synthetic: bytes served times each node’s configured `bandwidth_price`. `cost_per_byte` is `cost_units / bytes_served`. Origin `/__stats` matches edge origin fetches (11 = 11). Traffic did not all land on the cheapest node (`edge-b` price 0.8); replica-set steering also used `edge-a`. Failover (kill a node, `/locate` returns a live peer) is covered by `tests/failover.rs`, not by killing processes in this happy-path demo.

## How to talk about this

1. **Problem:** cheapest-node-only routing keeps caches cold; you still pay origin.
2. **Hard part:** byte-capped LRU + single-flight (one origin GET under a thundering herd) + replica-set steering with a hit hint.
3. **Where to point:** `phase2_single_flight_coalesces_concurrent_requests`, `phase5_hit_hint_can_override_price`, `failover_reroutes_to_a_live_peer`, `choose` vs `choose_cheapest` in `crates/steering`.

## Binaries

| Command | Role |
|---|---|
| `origin-mock BIND SIZE DELAY_MS` | Streaming fake origin; `GET /__stats` |
| `edge-node` | Data plane (`BIND`, `ORIGIN`, `NODE_ID`, `PRICE`, `CONTROL_PLANE`, `CACHE_BYTES`, `CAPACITY`, `RTT_MS`) |
| `control-plane BIND DNS_BIND` | Heartbeats, `/locate`, UDP DNS A records for `cdn.local` |
| `traffic-gen` | `LOCATE`, `ORIGIN`, `REQUESTS`, `KEYS`, `SIZE`; prints the `proof` block |

## Observability

Edges expose Prometheus text at `/__metrics` (including `edge_cost_units_total`) and JSON at `/__health`.

Optional Grafana (after the demo is running):

```bash
docker compose -f observability/docker-compose.yml up
```

Open Grafana on `http://localhost:3000`.

## Score function

A node is viable iff `healthy && inflight < capacity`.

Among the rendezvous-hash replica set of size `R=2`:

```text
score = w_price * bandwidth_price
      + w_lat   * ewma_rtt_ms
      + w_load  * utilization
      - w_hit   * local_hit_hint
```

Lowest score wins. If the replica set has no viable node, fall back to any viable node.

Baselines in `steering`: `choose_cheapest`, `choose_nearest`, `choose_hash_only`, and product `choose`.

See [docs/architecture.md](docs/architecture.md) for planes, cache-key rules, DNS vs locate, and failover.

## Not in v1

TLS termination, LFU, disk cache, HTTP/2, QUIC, BGP/anycast, production DNS, Kubernetes.
