# grpckit

[![Go Reference](https://pkg.go.dev/badge/github.com/boostgo/grpckit.svg)](https://pkg.go.dev/github.com/boostgo/grpckit)
[![Go Report Card](https://goreportcard.com/badge/github.com/boostgo/grpckit)](https://goreportcard.com/report/github.com/boostgo/grpckit)
[![CI](https://github.com/boostgo/grpckit/actions/workflows/ci.yml/badge.svg)](https://github.com/boostgo/grpckit/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**Production-ready gRPC servers and clients in five lines per side.**

grpckit is a thin wrapper around [`google.golang.org/grpc`](https://pkg.go.dev/google.golang.org/grpc)
that bakes in the things every service ends up writing by hand: panic recovery, structured
logging with [zerolog](https://github.com/rs/zerolog), trace ID propagation, health checks,
graceful shutdown that plays well with Kubernetes, safe retries and client-side load balancing.
It has no opinion on your contracts: register generated `*.pb.go` services and wrap the
connection with generated clients as usual.

## Features

- **Server in one call** with recovery, logging, trace ID and `grpc.health.v1` enabled by default.
- **Graceful, idempotent shutdown**: health goes `NOT_SERVING`, a configurable pause lets load
  balancers notice, then `GracefulStop` with a hard ceiling.
- **Keepalive that just works**: a server enforcement policy compatible with client keepalive
  pings (no more `GOAWAY too_many_pings`), plus optional connection-age rotation.
- **Client with round-robin balancing** over all DNS addresses (headless Services).
- **Safe retries**: retries on `UNAVAILABLE` apply only to methods you list explicitly as idempotent;
  invalid retry configs fail fast at dial time.
- **Per-call unary timeout** that never cuts long-lived streams.
- **Message size limits and gzip** on both sides.
- **Trace ID propagation** across services via `x-trace-id` metadata and the `trace_id` context key,
  shared with [`httpkit`](https://github.com/boostgo/httpkit) and
  [`kafkakit`](https://github.com/boostgo/kafkakit).
- **Escape hatches** for custom interceptors and raw `grpc.ServerOption` / `grpc.DialOption`.

## Installation

```sh
go get github.com/boostgo/grpckit
```

Requires Go 1.22 or newer.

## Quick start

### Server

```go
srv, err := grpckit.NewServer(logger,
    grpckit.WithAddr(":9090"),
    grpckit.WithReflection(false), // enable in development only
)
if err != nil {
    return err
}

greeterv1.RegisterGreeterServer(srv.Registrar(), &greeter{})

if err := srv.Start(ctx); err != nil { // non-blocking; returns bind errors immediately
    return err
}

<-ctx.Done()
shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
defer cancel()
_ = srv.Stop(shutdownCtx) // graceful
```

### Client

```go
conn, err := grpckit.Dial(ctx, logger,
    grpckit.WithTarget("dns:///greeter.default.svc.cluster.local:9090"),
    grpckit.WithKeepAlive(30*time.Second),
    grpckit.WithPerCallTimeout(2*time.Second),
)
if err != nil {
    return err
}
defer conn.Close()

client := greeterv1.NewGreeterClient(conn)
```

Create **one connection per process** and share it: HTTP/2 multiplexes every RPC over it.

Complete programs live in [`examples/server`](examples/server/main.go) and
[`examples/client`](examples/client/main.go).

## Usage

### Graceful shutdown

`Stop(ctx)` performs:

1. health status -> `NOT_SERVING`;
2. a pause of `WithShutdownPause` (default 5s) so clients and load balancers notice;
3. `GracefulStop`, waiting for in-flight RPCs.

If ctx is done first, the server is stopped forcefully. If ctx has **no deadline**, waiting is
capped by `WithStopTimeout` (default 20s). Pause plus ceiling stay below the 30s default
Kubernetes `terminationGracePeriodSeconds`. Calling `Stop` twice, or before `Start`, is safe.

```go
srv, _ := grpckit.NewServer(logger,
    grpckit.WithShutdownPause(3*time.Second),
    grpckit.WithStopTimeout(15*time.Second),
)
```

Use `srv.Health()` to change health statuses yourself, and `srv.Addr()` to get the bound address
(handy with `WithAddr("127.0.0.1:0")` in tests).

### Keepalive and connection rotation

The server always sets a keepalive enforcement policy: pings no more often than every **10s**,
including without active streams (`WithKeepaliveEnforcement(minTime, permitWithoutStream)`).
Without it, the grpc-go default (5 minutes, no pings outside streams) disconnects clients that use
`WithKeepAlive` with `GOAWAY too_many_pings`.

`WithMaxConnectionAge(age, grace)` (disabled by default) makes the server send GOAWAY
periodically: clients reconnect and re-resolve DNS, so round-robin discovers new pods. `grace` is
how long long-running RPCs and streams may continue; for long streams it should cover their
duration.

```go
srv, _ := grpckit.NewServer(logger,
    grpckit.WithMaxConnectionAge(30*time.Minute, time.Minute),
)
conn, _ := grpckit.Dial(ctx, logger,
    grpckit.WithTarget(target),
    grpckit.WithKeepAlive(30*time.Second),
)
```

### Retries

Retries on `UNAVAILABLE` are enabled only when `WithRetry(n, backoff)` (`2 <= n <= 5`,
`backoff >= 1ms`, `0` means 100ms) is combined with a non-empty
`WithRetryMethods("/<pkg.Service>/<Method>", ...)`. Without a method list, `WithRetry` does nothing
and `Dial` logs a warning: retrying a non-idempotent call (create, reserve, bulk insert, counters)
produces duplicates. Invalid values or method names make `Dial` return `ErrInvalidRetry`.

```go
conn, err := grpckit.Dial(ctx, logger,
    grpckit.WithTarget(target),
    grpckit.WithRetry(3, 100*time.Millisecond),
    grpckit.WithRetryMethods( // idempotent methods only!
        greeterv1.Greeter_GetGreeting_FullMethodName, // "/greeter.v1.Greeter/GetGreeting"
    ),
)
if errors.Is(err, grpckit.ErrInvalidRetry) {
    // fix the configuration
}
```

Streams can only be retried before the first response. grpc buffers at most **256 KiB** of a
request for retries; larger requests are not retried by the transport.

### Message size and compression

grpc defaults to **4 MiB on receive** (no practical limit on send). Large bulk responses or stream
pages fail with `ResourceExhausted`, and server and client limits are **independent**, so raise
both sides:

```go
srv, _ := grpckit.NewServer(logger,
    grpckit.WithMaxRecvMsgSize(16<<20),
    grpckit.WithMaxSendMsgSize(16<<20),
)
conn, _ := grpckit.Dial(ctx, logger,
    grpckit.WithTarget(target),
    grpckit.WithClientMaxRecvMsgSize(16<<20),
    grpckit.WithClientMaxSendMsgSize(16<<20),
    grpckit.WithGzip(true),
)
```

Client limits are applied via `grpc.WithDefaultCallOptions`, i.e. to unary calls and streams, and
are measured on the **decompressed** message. The gzip decompressor is always registered when you
import grpckit, so you can roll out `WithGzip` one side at a time.

### Tracing

The client interceptor copies the trace ID from the caller's context into the `x-trace-id`
metadata entry; the server interceptor puts it back into the handler's context under
`grpckit.TraceKey` (`"trace_id"`). Incoming IDs are truncated to 128 bytes.

```go
ctx = grpckit.ContextWithTraceID(ctx, traceID)
resp, err := client.GetGreeting(ctx, req)

// on the server
func (g *greeter) GetGreeting(ctx context.Context, req *greeterv1.GetGreetingRequest) (*greeterv1.GetGreetingResponse, error) {
    traceID := grpckit.TraceIDFromContext(ctx)
    ...
}
```

### Logging

Every call is logged with `method`, `code`, `took` and `trace_id`. Successful calls are logged at
**Debug** (`WithSuccessLogLevel` on the server, `WithClientSuccessLogLevel` on the client);
client-side errors (`NotFound`, `InvalidArgument`, `AlreadyExists`, `ResourceExhausted`,
`FailedPrecondition`, `Canceled`) at Warn, everything else at Error. Panics are recovered, logged
with a stack trace and returned as `codes.Internal`.

### Health checks

```go
ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
if err := grpckit.HealthCheck(ctx, conn, ""); err != nil {
    // errors.Is(err, grpckit.ErrHealthCheck): transport error, status.Code(err) is preserved
    // errors.Is(err, grpckit.ErrNotServing):  server reachable but not SERVING
}
```

`HealthCheck` is never retried, which makes it a good half-open probe for a circuit breaker.

### Kubernetes load balancing

For client-side round-robin, expose the gRPC port through a **headless Service**
(`clusterIP: None`): DNS returns every pod IP and the `round_robin` policy balances requests
across them without an external load balancer. Use the target
`dns:///<service>.<namespace>.svc.cluster.local:9090`.

## Options reference

### Server (`NewServer`)

| Option | Default | Description |
|---|---|---|
| `WithAddr(addr)` | `":9090"` | Listen address. Empty -> `ErrAddrRequired`. |
| `WithRecovery(bool)` | `true` | Recover panics, return `codes.Internal`. |
| `WithLogging(bool)` | `true` | Log every unary/stream call. |
| `WithTraceID(bool)` | `true` | Copy `x-trace-id` metadata into ctx. |
| `WithHealth(bool)` | `true` | Register `grpc.health.v1.Health`. |
| `WithReflection(bool)` | `false` | Server reflection (grpcurl); development only. |
| `WithMaxRecvMsgSize(n)` | grpc default (4 MiB) | Max inbound message size in bytes. |
| `WithMaxSendMsgSize(n)` | grpc default | Max outbound message size in bytes. |
| `WithKeepaliveEnforcement(min, permit)` | `10s`, `true` | Allowed client ping rate. |
| `WithMaxConnectionAge(age, grace)` | disabled | Periodic GOAWAY for connection rotation. |
| `WithShutdownPause(d)` | `5s` | Pause between `NOT_SERVING` and `GracefulStop`. |
| `WithStopTimeout(d)` | `20s` | Stop ceiling when ctx has no deadline. |
| `WithSuccessLogLevel(level)` | `Debug` | Log level for `codes.OK`. |
| `WithServerUnaryInterceptors(...)` | none | Extra unary interceptors (after built-ins). |
| `WithServerStreamInterceptors(...)` | none | Extra stream interceptors (after built-ins). |
| `WithServerOptions(...)` | none | Raw `grpc.ServerOption` escape hatch. |

### Client (`Dial`)

| Option | Default | Description |
|---|---|---|
| `WithTarget(target)` | required | `dns:///host:port` or `host:port`. Empty -> `ErrTargetRequired`. |
| `WithRoundRobin(bool)` | `true` | Client-side `round_robin` balancing. |
| `WithKeepAlive(period)` | disabled | Keepalive ping period. |
| `WithKeepAliveTimeout(d)` | `10s` | Keepalive ack timeout. |
| `WithPerCallTimeout(d)` | none | Deadline per unary call (not streams). |
| `WithRetry(n, backoff)` | off | Attempts (2-5) and initial backoff for `UNAVAILABLE`. |
| `WithRetryMethods(...)` | none | Full method names that may be retried. |
| `WithClientMaxRecvMsgSize(n)` | grpc default (4 MiB) | Max received message size. |
| `WithClientMaxSendMsgSize(n)` | grpc default | Max sent message size. |
| `WithGzip(bool)` | `false` | Gzip outgoing messages. |
| `WithInsecure()` | plaintext | Plaintext transport (default). |
| `WithTLS(cfg)` | none | TLS transport. |
| `WithClientLogging(bool)` | `true` | Log every unary call. |
| `WithTraceIDInjection(bool)` | `true` | Send the ctx trace ID as `x-trace-id`. |
| `WithBlock()` | `false` | Wait in `Dial` until the connection is Ready. |
| `WithClientSuccessLogLevel(level)` | `Debug` | Log level for `codes.OK`. |
| `WithClientUnaryInterceptors(...)` | none | Extra unary interceptors (after built-ins). |
| `WithClientStreamInterceptors(...)` | none | Extra stream interceptors (after built-ins). |
| `WithDialOptions(...)` | none | Raw `grpc.DialOption` escape hatch. |

Interceptor order: server `recovery -> trace ID -> logging -> yours`; client
`trace ID -> timeout -> logging -> yours`.

## Contributing

Issues and pull requests are welcome. Before opening a PR:

```sh
make check      # go vet + golangci-lint + go test -race
make test-short # skip the ~35s keepalive tests while iterating
make cover      # coverage summary
```

Please add tests for behaviour changes and keep the public API documented.

## License

[MIT](LICENSE) © 2026 boostgo
