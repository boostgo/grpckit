package grpckit

import (
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
)

const (
	// maxRetryAttempts is the maxAttempts ceiling; grpc silently caps larger values at 5.
	maxRetryAttempts = 5
	minRetryBackoff  = time.Millisecond
)

// ClientOptions is the client connection configuration. Defaults: round-robin enabled, plaintext
// (insecure) transport, logging and trace ID injection enabled, lazy connection (Block=false).
// Use WithTLS for connections that leave a trusted network.
type ClientOptions struct {
	Target string // dns:///host:port or host:port; required

	RoundRobin bool // client-side round_robin balancing across all DNS addresses, default true

	KeepAlive        time.Duration // keepalive ping period; 0 -> disabled
	KeepAliveTimeout time.Duration // keepalive ping ack timeout, default 10s (when KeepAlive > 0)

	PerCallTimeout time.Duration // deadline per unary call (NOT applied to streams); 0 -> none
	MaxRetries     int           // attempts on UNAVAILABLE (including the first); < 2 -> no retries
	RetryBackoff   time.Duration // initial retry backoff, default 100ms
	// RetryMethods are full method names ("/pkg.Service/Method") that may be retried; empty -> no retries.
	RetryMethods []string

	MaxRecvMsgSize int  // max size of RECEIVED messages in bytes; 0 -> grpc default (4 MiB)
	MaxSendMsgSize int  // max size of SENT messages in bytes; 0 -> grpc default (unlimited)
	Gzip           bool // gzip-compress outgoing messages; default false

	Insecure  bool        // plaintext transport, default true; WithTLS overrides it
	TLSConfig *tls.Config // when set, TLS is used instead of plaintext

	Logging        bool // client logging interceptor, default true
	TraceInjection bool // copies the trace ID from ctx into outgoing metadata, default true
	Block          bool // block in Dial until Ready (bounded by ctx), default false

	// SuccessLogLevel is the log level of successful calls (codes.OK), default zerolog.DebugLevel.
	SuccessLogLevel zerolog.Level

	// Escape hatches.
	UnaryInterceptors  []grpc.UnaryClientInterceptor
	StreamInterceptors []grpc.StreamClientInterceptor
	DialOptions        []grpc.DialOption
}

// ClientOption configures a client connection. Pass options to Dial.
type ClientOption func(*ClientOptions)

func newClientOptions() *ClientOptions {
	return &ClientOptions{
		RoundRobin:       true,
		KeepAliveTimeout: 10 * time.Second,
		RetryBackoff:     100 * time.Millisecond,
		Insecure:         true,
		Logging:          true,
		TraceInjection:   true,
		SuccessLogLevel:  zerolog.DebugLevel,
	}
}

func (o *ClientOptions) validate() error {
	if o.Target == "" {
		return ErrTargetRequired
	}
	for _, m := range o.RetryMethods {
		if _, _, err := splitMethod(m); err != nil {
			return err
		}
	}
	if o.retryEnabled() {
		if o.MaxRetries > maxRetryAttempts {
			return fmt.Errorf("%w: maxAttempts %d > %d", ErrInvalidRetry, o.MaxRetries, maxRetryAttempts)
		}
		if o.RetryBackoff < minRetryBackoff {
			return fmt.Errorf("%w: backoff %s < %s", ErrInvalidRetry, o.RetryBackoff, minRetryBackoff)
		}
	}
	return nil
}

// retryEnabled reports whether retries are actually on: at least 2 attempts and a method list.
func (o *ClientOptions) retryEnabled() bool {
	return o.MaxRetries >= 2 && len(o.RetryMethods) > 0
}

// splitMethod splits "/<pkg.Service>/<Method>" into service and method.
func splitMethod(full string) (service, method string, err error) {
	rest, ok := strings.CutPrefix(full, "/")
	service, method, found := strings.Cut(rest, "/")
	if !ok || !found || service == "" || method == "" || strings.Contains(method, "/") {
		return "", "", fmt.Errorf("%w: method %q: want \"/<pkg.Service>/<Method>\"", ErrInvalidRetry, full)
	}
	return service, method, nil
}

// WithTarget sets the connection target. For round-robin in Kubernetes point it at a headless
// Service: "dns:///greeter.<namespace>.svc.cluster.local:9090".
func WithTarget(target string) ClientOption {
	return func(o *ClientOptions) { o.Target = target }
}

// WithRoundRobin enables or disables round_robin load balancing.
func WithRoundRobin(enabled bool) ClientOption {
	return func(o *ClientOptions) { o.RoundRobin = enabled }
}

// WithKeepAlive enables keepalive pings with the given period.
func WithKeepAlive(period time.Duration) ClientOption {
	return func(o *ClientOptions) { o.KeepAlive = period }
}

// WithKeepAliveTimeout sets how long to wait for a keepalive ping acknowledgement.
func WithKeepAliveTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) { o.KeepAliveTimeout = timeout }
}

// WithPerCallTimeout sets a deadline for EVERY unary call. It does not apply to streams, which
// are often long-lived.
func WithPerCallTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) { o.PerCallTimeout = timeout }
}

// WithRetry sets the parameters of grpc's built-in retries on UNAVAILABLE: maxAttempts includes
// the first attempt and must not exceed 5; backoff is the initial backoff, at least 1ms (0 keeps
// the 100ms default). On its own it enables nothing: only methods listed with WithRetryMethods are
// retried, and without that list there are no retries (Dial logs a warning). Out-of-range values
// make Dial return ErrInvalidRetry.
func WithRetry(maxAttempts int, backoff time.Duration) ClientOption {
	return func(o *ClientOptions) {
		o.MaxRetries = maxAttempts
		if backoff > 0 {
			o.RetryBackoff = backoff
		}
	}
}

// WithRetryMethods lists the methods that may be retried on UNAVAILABLE, as full gRPC names
// "/<pkg.Service>/<Method>" (the <Service>_<Method>_FullMethodName constants in generated code).
// List IDEMPOTENT methods only: retrying a non-idempotent call (create, reserve, bulk insert)
// produces duplicates. Works together with WithRetry; duplicates are collapsed and an invalid
// format makes Dial return ErrInvalidRetry.
func WithRetryMethods(methods ...string) ClientOption {
	return func(o *ClientOptions) { o.RetryMethods = append(o.RetryMethods, methods...) }
}

// WithClientSuccessLogLevel sets the log level of successful (codes.OK) client calls.
// Default zerolog.DebugLevel; error codes are still logged at Warn/Error.
func WithClientSuccessLogLevel(level zerolog.Level) ClientOption {
	return func(o *ClientOptions) { o.SuccessLogLevel = level }
}

// WithClientMaxRecvMsgSize limits the size of messages RECEIVED by the client, in bytes. Bulk
// responses and large stream pages easily exceed the default 4 MiB and fail with
// ResourceExhausted. 0 (default) keeps the grpc limit.
func WithClientMaxRecvMsgSize(size int) ClientOption {
	return func(o *ClientOptions) { o.MaxRecvMsgSize = size }
}

// WithClientMaxSendMsgSize limits the size of messages SENT by the client, in bytes, which
// matters for large bulk requests. 0 (default) keeps the grpc limit.
func WithClientMaxSendMsgSize(size int) ClientOption {
	return func(o *ClientOptions) { o.MaxSendMsgSize = size }
}

// WithGzip enables gzip compression of outgoing messages (default false). Compressed responses
// are always decompressed because importing this package registers the compressor, so you can
// enable compression on one side at a time. grpc applies MaxRecvMsgSize/MaxSendMsgSize to the
// DECOMPRESSED message, so compression does not replace raising the limits.
func WithGzip(enabled bool) ClientOption {
	return func(o *ClientOptions) { o.Gzip = enabled }
}

// WithInsecure selects a plaintext transport (and clears any TLS config).
func WithInsecure() ClientOption {
	return func(o *ClientOptions) {
		o.Insecure = true
		o.TLSConfig = nil
	}
}

// WithTLS enables TLS with the given config.
func WithTLS(config *tls.Config) ClientOption {
	return func(o *ClientOptions) {
		o.Insecure = false
		o.TLSConfig = config
	}
}

// WithClientLogging enables or disables the client logging interceptor.
func WithClientLogging(enabled bool) ClientOption {
	return func(o *ClientOptions) { o.Logging = enabled }
}

// WithTraceIDInjection enables or disables copying the trace ID from ctx into outgoing metadata.
func WithTraceIDInjection(enabled bool) ClientOption {
	return func(o *ClientOptions) { o.TraceInjection = enabled }
}

// WithBlock makes Dial block until the connection is Ready (or ctx is done).
func WithBlock() ClientOption {
	return func(o *ClientOptions) { o.Block = true }
}

// WithClientUnaryInterceptors appends custom unary interceptors (run after the built-in ones).
func WithClientUnaryInterceptors(interceptors ...grpc.UnaryClientInterceptor) ClientOption {
	return func(o *ClientOptions) { o.UnaryInterceptors = append(o.UnaryInterceptors, interceptors...) }
}

// WithClientStreamInterceptors appends custom stream interceptors (run after the built-in ones).
func WithClientStreamInterceptors(interceptors ...grpc.StreamClientInterceptor) ClientOption {
	return func(o *ClientOptions) { o.StreamInterceptors = append(o.StreamInterceptors, interceptors...) }
}

// WithDialOptions passes arbitrary grpc.DialOption values through (escape hatch).
func WithDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *ClientOptions) { o.DialOptions = append(o.DialOptions, opts...) }
}
