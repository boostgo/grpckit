# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] - 2026-11-20

First public release.

### Added

- `NewServer` with built-in recovery, logging (zerolog), trace ID and health interceptors/services,
  optional reflection, non-blocking `Start` and graceful, idempotent `Stop` (health `NOT_SERVING`
  -> `ShutdownPause` -> `GracefulStop`, capped by `StopTimeout` when ctx has no deadline).
- Server options: address, interceptor toggles, message size limits, keepalive enforcement
  (default 10s, pings without streams allowed), `MaxConnectionAge` rotation, shutdown pause,
  stop timeout, success log level, custom interceptors and raw `grpc.ServerOption` escape hatch.
- `Dial` with round-robin balancing (default on), keepalive, per-call unary timeout, retries on
  `UNAVAILABLE` restricted to an explicit list of idempotent methods, message size limits, gzip,
  TLS, `WithBlock`, success log level and custom interceptors / raw `grpc.DialOption` escape hatch.
- `HealthCheck` probe that preserves the gRPC status code.
- Trace ID propagation via the `x-trace-id` metadata key and the `trace_id` context key
  (`TraceIDFromContext`, `ContextWithTraceID`); incoming IDs are capped at 128 bytes on a rune
  boundary.
- Sentinel errors: `ErrAddrRequired`, `ErrServerListen`, `ErrTargetRequired`, `ErrClientDial`,
  `ErrInvalidRetry`, `ErrClientNotReady`, `ErrHealthCheck`, `ErrNotServing`.
- Runnable examples, `examples/server` and `examples/client` programs, CI, Makefile.

