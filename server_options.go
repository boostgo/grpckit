package grpckit

import (
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

const (
	defaultKeepaliveMinTime = 10 * time.Second
	defaultShutdownPause    = 5 * time.Second
	defaultStopTimeout      = 20 * time.Second
)

// ServerOptions is the gRPC server configuration. The defaults are safe: the server starts with
// recovery, logging, trace ID propagation and health enabled, and reflection disabled.
type ServerOptions struct {
	Addr       string // listen address, default ":9090"
	Recovery   bool   // panic recovery interceptor (panic -> codes.Internal), default true
	Logging    bool   // logging interceptor (zerolog), default true
	TraceID    bool   // copies the trace ID from incoming metadata into ctx, default true
	Health     bool   // registers the grpc.health.v1 service, default true
	Reflection bool   // server reflection (for grpcurl); default false, enable in development only

	MaxRecvMsgSize int // max inbound message size in bytes; 0 -> grpc default (4 MiB)
	MaxSendMsgSize int // max outbound message size in bytes; 0 -> grpc default

	// Server-side keepalive enforcement policy. Clients configured with WithKeepAlive ping while
	// idle too; the grpc-go default (MinTime 5m, no pings without active streams) would close such
	// connections with GOAWAY "too_many_pings".
	KeepaliveMinTime             time.Duration // minimum interval between client pings, default 10s
	KeepalivePermitWithoutStream bool          // allow pings without active streams, default true

	// MaxConnectionAge is the maximum connection age after which the server sends GOAWAY. The
	// client reconnects and re-resolves DNS, so round_robin picks up new backends. 0 disables it.
	MaxConnectionAge time.Duration
	// MaxConnectionAgeGrace is how long in-flight RPCs and streams may continue after GOAWAY.
	MaxConnectionAgeGrace time.Duration

	// ShutdownPause is the pause in Stop between NOT_SERVING and GracefulStop, giving clients
	// time to notice the status. 0 means no pause. Default 5s.
	ShutdownPause time.Duration
	// StopTimeout caps the wait in Stop when ctx has no deadline; afterwards the server is
	// stopped forcefully. 0 means no cap. Default 20s (pause + cap stay below the 30s default
	// Kubernetes terminationGracePeriodSeconds).
	StopTimeout time.Duration

	// SuccessLogLevel is the log level for successful calls (codes.OK), default zerolog.DebugLevel.
	SuccessLogLevel zerolog.Level

	// Escape hatches for non-standard needs (applied AFTER the built-in interceptors).
	UnaryInterceptors  []grpc.UnaryServerInterceptor
	StreamInterceptors []grpc.StreamServerInterceptor
	ServerOptions      []grpc.ServerOption
}

// ServerOption configures a Server. Pass options to NewServer.
type ServerOption func(*ServerOptions)

func newServerOptions() *ServerOptions {
	return &ServerOptions{
		Addr:       ":9090",
		Recovery:   true,
		Logging:    true,
		TraceID:    true,
		Health:     true,
		Reflection: false,

		KeepaliveMinTime:             defaultKeepaliveMinTime,
		KeepalivePermitWithoutStream: true,
		ShutdownPause:                defaultShutdownPause,
		StopTimeout:                  defaultStopTimeout,
		SuccessLogLevel:              zerolog.DebugLevel,
	}
}

func (o *ServerOptions) validate() error {
	if o.Addr == "" {
		return ErrAddrRequired
	}
	return nil
}

// WithAddr sets the listen address (for example ":9090").
func WithAddr(addr string) ServerOption {
	return func(o *ServerOptions) { o.Addr = addr }
}

// WithRecovery enables or disables the panic recovery interceptor.
func WithRecovery(enabled bool) ServerOption {
	return func(o *ServerOptions) { o.Recovery = enabled }
}

// WithLogging enables or disables the server logging interceptor.
func WithLogging(enabled bool) ServerOption {
	return func(o *ServerOptions) { o.Logging = enabled }
}

// WithTraceID enables or disables copying the trace ID from incoming metadata into ctx.
func WithTraceID(enabled bool) ServerOption {
	return func(o *ServerOptions) { o.TraceID = enabled }
}

// WithHealth enables or disables registration of the grpc.health.v1 service.
func WithHealth(enabled bool) ServerOption {
	return func(o *ServerOptions) { o.Health = enabled }
}

// WithReflection enables server reflection (for grpcurl). Keep it disabled in production.
func WithReflection(enabled bool) ServerOption {
	return func(o *ServerOptions) { o.Reflection = enabled }
}

// WithMaxRecvMsgSize limits the size of inbound messages, in bytes.
func WithMaxRecvMsgSize(size int) ServerOption {
	return func(o *ServerOptions) { o.MaxRecvMsgSize = size }
}

// WithMaxSendMsgSize limits the size of outbound messages, in bytes.
func WithMaxSendMsgSize(size int) ServerOption {
	return func(o *ServerOptions) { o.MaxSendMsgSize = size }
}

// WithServerUnaryInterceptors appends custom unary interceptors (run after the built-in ones).
func WithServerUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(o *ServerOptions) { o.UnaryInterceptors = append(o.UnaryInterceptors, interceptors...) }
}

// WithServerStreamInterceptors appends custom stream interceptors (run after the built-in ones).
func WithServerStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(o *ServerOptions) { o.StreamInterceptors = append(o.StreamInterceptors, interceptors...) }
}

// WithServerOptions passes arbitrary grpc.ServerOption values through (escape hatch).
func WithServerOptions(opts ...grpc.ServerOption) ServerOption {
	return func(o *ServerOptions) { o.ServerOptions = append(o.ServerOptions, opts...) }
}

// WithKeepaliveEnforcement sets the server policy for client keepalive pings: minTime is the
// minimum interval between pings, permitWithoutStream allows pings without active streams.
// Default: 10s and true, which is compatible with any client (grpc-go itself raises the client
// keepalive to at least 10s). Pings more frequent than the policy cause GOAWAY "too_many_pings".
func WithKeepaliveEnforcement(minTime time.Duration, permitWithoutStream bool) ServerOption {
	return func(o *ServerOptions) {
		o.KeepaliveMinTime = minTime
		o.KeepalivePermitWithoutStream = permitWithoutStream
	}
}

// WithMaxConnectionAge enables connection rotation: once a connection reaches age the server
// sends GOAWAY, and grace is the time left for in-flight RPCs and streams before the connection
// is closed. The client reconnects and re-resolves DNS, so round_robin sees new backends.
// Default: 0 (disabled). For services with long-lived streams grace should cover the stream
// lifetime.
func WithMaxConnectionAge(age, grace time.Duration) ServerOption {
	return func(o *ServerOptions) {
		o.MaxConnectionAge = age
		o.MaxConnectionAgeGrace = grace
	}
}

// WithShutdownPause sets the pause in Stop between switching health to NOT_SERVING and
// GracefulStop, so clients and load balancers notice the status. 0 means no pause. Default 5s.
func WithShutdownPause(d time.Duration) ServerOption {
	return func(o *ServerOptions) { o.ShutdownPause = d }
}

// WithStopTimeout caps graceful shutdown for a ctx without a deadline: once it elapses, Stop
// forcefully terminates the remaining RPCs and streams. A ctx deadline, when present, takes
// precedence. 0 means no cap. Default 20s.
func WithStopTimeout(d time.Duration) ServerOption {
	return func(o *ServerOptions) { o.StopTimeout = d }
}

// WithSuccessLogLevel sets the log level of successful (codes.OK) unary and stream calls.
// Default zerolog.DebugLevel; error codes are still logged at Warn/Error.
func WithSuccessLogLevel(level zerolog.Level) ServerOption {
	return func(o *ServerOptions) { o.SuccessLogLevel = level }
}
