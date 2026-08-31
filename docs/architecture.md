# Skynet architecture

## Planes

- **Data plane** (`edge-node`): GET proxy, byte-capped LRU, single-flight, tee-to-client while filling cache, Prometheus metrics, `/__health`. When `control_plane` is set, the library posts `/__health` snapshots to `/heartbeat` every 250ms (including `hot_keys` from the local LRU).
- **Origin** (`origin-mock`): chunked `'a'` payloads, optional per-chunk delay, per-path counters, optional 5xx, `GET /__stats` (`total_hits`).
- **Control plane**: node registry from heartbeats; nodes older than `stale_after` are dropped. `GET /locate?key=` (or `path=`) runs the steering chooser. UDP DNS A-record for `cdn.local` uses the **same** chooser with key `cdn.local` (host-level, not per-object).
- **Test / load**: integration tests plus `traffic-gen` (Zipf keys). After load, traffic-gen scrapes `/nodes`, each edge `/__metrics`, and origin `/__stats` and prints a `proof` block.

## Request path

1. Client calls `GET /locate?key=/obj/k` (demo / traffic-gen). DNS is optional and host-level only.
2. Control plane builds snapshots, sets `has_key` from heartbeat `hot_keys`, runs rendezvous hash replica set then min score.
3. Client GETs the chosen edge.
4. Edge: cache hit serves memory; miss leader fetches origin once; waiters block on the flight; 2xx objects ≤ max size enter LRU; 5xx is never stored.

## Cache key

The cache key is the **URI path only** (for example `/obj/hot`), not the query string. `?size=` is origin parameterization for the lab. Tests and the demo must not vary size on the same path; doing so would serve the wrong body from cache.

## Cache rules

- Capacity is **bytes**, not object count.
- LRU victim is the least recently used key.
- Objects larger than `cache_max_object_bytes` are streamed but not stored.
- Leader tees origin chunks to its client and buffers for waiters / insert.
- Independent nodes do **not** share a cache (Phase 3 test: two edges ⇒ two origin GETs).

## Steering vs cheapest-node

Picking the globally cheapest PoP keeps caches cold. Skynet hashes the object key onto `R` replicas (rendezvous hashing), then applies price, RTT, load, and hit hint **inside that set**.

Named baselines in `crates/steering` (pure library, no HTTP):

- `choose_cheapest` — min `bandwidth_price` among viable nodes
- `choose_nearest` — min `ewma_rtt_ms`
- `choose_hash_only` — first viable node in the replica set
- `choose` — replica set, then score including hit hint (the product)

## Failover

If a node stops heartbeating, it ages out after `stale_after` (default 3s; tests use ~300ms). `/locate` then returns another **viable** node (200), not an empty 503, as long as at least one peer remains. That reroute is `tests/failover.rs`. A single remaining node that also goes stale yields 503 (`phase5_stale_heartbeat_is_dropped`).

## Proof production

Synthetic cost: on each byte served, the edge increments `edge_cost_units_total` by `bytes * bandwidth_price`. traffic-gen does not invent a second formula; it scrapes that counter. `cost_per_byte = cost_units / bytes_served`. Origin `/__stats.total_hits` is the ground truth for origin load and should match `sum(edge_origin_fetches_total)` after a run (single-flight means those two stay aligned).

## Cut from v1

TLS termination, LFU, disk cache, HTTP/2, QUIC, BGP/anycast, production DNS, Kubernetes. Grafana is optional screenshots; Prometheus text + tests + `./scripts/demo.sh` are the proof.
