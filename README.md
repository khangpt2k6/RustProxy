# RustProxy

High performance async reverse proxy built on Tokio and Hyper.

- request routing to a pool of backend services
- load balancing: round robin or least connections
- active health checks with fall/rise thresholds (flap protection)
- gRPC control plane (tonic) - add/remove backends at runtime, no restart
- Prometheus metrics: request counts, latency histograms, in-flight connections, healthy backend gauge
- built-in demo backend and load generator binaries

## Architecture

```
                      +-------------------+
 client ---- HTTP --->|      proxy        |----> backend 1
                      |  (tokio + hyper)  |----> backend 2
                      |                   |----> backend 3
                      +-------------------+
                        |       |      |
                 :9090 /metrics | :50051 gRPC admin
                                |
                        health checker task
                     (probes /health every 5s)
```

Every accepted connection is served on its own tokio task. Upstream requests
go through a shared hyper client with a warm connection pool, so hot paths
don't pay connect cost. Backend state (health, in-flight counts) is lock-free
atomics; the backend list itself is behind an RwLock that's only write-locked
by the admin API.

## Quick start

```sh
# 3 demo backends
cargo run --release --bin demo-backend 9001 &
cargo run --release --bin demo-backend 9002 &
cargo run --release --bin demo-backend 9003 &

# the proxy
cargo run --release --bin rustproxy -- --config config.yaml

# traffic round-robins across backends
curl http://127.0.0.1:8080/
curl http://127.0.0.1:9090/metrics
```

Or with docker:

```sh
docker compose up --build
# proxy on :8080, metrics on :9090, grafana-ready prometheus on :9091
```

## Runtime backend management (gRPC)

```sh
cargo run --example admin_client -- list
cargo run --example admin_client -- add http://127.0.0.1:9004
cargo run --example admin_client -- remove http://127.0.0.1:9004
```

New backends take traffic immediately; removed ones drain naturally since
in-flight requests hold their own Arc to the backend.

## Config

```yaml
listen: 0.0.0.0:8080
metrics_listen: 0.0.0.0:9090
admin_listen: 0.0.0.0:50051
strategy: round_robin # or least_connections

backends:
  - addr: http://127.0.0.1:9001
  - addr: http://127.0.0.1:9002

health_check:
  interval_secs: 5
  timeout_secs: 2
  path: /health
  fall: 3   # consecutive fails before marking down
  rise: 2   # consecutive oks before marking back up
```

## Load testing

There's a bundled load generator:

```sh
cargo run --release --bin loadgen -- http://127.0.0.1:8080/ 512 15
```

Numbers from a Windows laptop over loopback (proxy + 3 backends + loadgen all
on the same machine, so this is a floor not a ceiling):

| concurrency | throughput | errors | p50     | p99    |
|-------------|------------|--------|---------|--------|
| 512         | ~30k req/s | 0      | <=25ms  | <=50ms |
| 2048        | ~21k req/s | 0      | <=100ms | <=1s   |

Fun bug found while benching: with the default 64 idle upstream conns per
host, high-concurrency runs churned connections fast enough to exhaust
ephemeral ports with TIME_WAIT sockets (925 errors at 2048 conns). Bumping
the warm pool to 1024 got it back to zero errors.

## Metrics

| metric | type | labels |
|--------|------|--------|
| `rustproxy_requests_total` | counter | backend, status |
| `rustproxy_request_duration_seconds` | histogram | backend |
| `rustproxy_active_connections` | gauge | backend |
| `rustproxy_healthy_backends` | gauge | |
| `rustproxy_errors_total` | counter | kind |

## Tests

```sh
cargo test
```

Covers balancer strategies, health fall/rise state machine, conn guard
accounting, and pool add/remove.
