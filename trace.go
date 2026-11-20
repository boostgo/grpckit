package grpckit

import (
	"context"
	"unicode/utf8"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

const (
	// TraceKey is the context.Context key under which the trace ID is stored. It intentionally
	// matches the key used by github.com/boostgo/httpkit and github.com/boostgo/kafkakit
	// ("trace_id") so that end-to-end traces line up in logs across HTTP, Kafka and gRPC. The key
	// is a plain string for the same interoperability reason.
	TraceKey = "trace_id"
	// TraceMetadataKey is the gRPC metadata key that carries the trace ID (metadata keys are
	// always lower-case).
	TraceMetadataKey = "x-trace-id"
)

// TraceIDFromContext returns the trace ID stored in ctx under TraceKey, or an empty string if
// there is none. It is populated by HTTP middleware, by your own handlers, or by the server-side
// trace interceptor of this package.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(TraceKey).(string); ok {
		return v
	}
	return ""
}

// ContextWithTraceID returns a copy of ctx that carries traceID under TraceKey.
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, TraceKey, traceID) //nolint:staticcheck // string key by cross-library convention
}

// maxTraceIDLen is the maximum length, in bytes, of an incoming trace ID.
const maxTraceIDLen = 128

// traceIDFromIncomingMD reads the trace ID from incoming gRPC metadata (server side). Values
// longer than maxTraceIDLen (128) bytes are truncated on a rune boundary, so UTF-8 stays valid.
func traceIDFromIncomingMD(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if values := md.Get(TraceMetadataKey); len(values) > 0 {
		return truncateTraceID(values[0])
	}
	return ""
}

func truncateTraceID(id string) string {
	if len(id) <= maxTraceIDLen {
		return id
	}
	n := maxTraceIDLen
	for n > 0 && !utf8.RuneStart(id[n]) {
		n--
	}
	return id[:n]
}

// logEventByCode picks the log level for a gRPC status code: OK uses successLevel, client-side
// or expected errors use Warn, server/infrastructure errors use Error.
func logEventByCode(logger zerolog.Logger, code codes.Code, successLevel zerolog.Level) *zerolog.Event {
	switch code {
	case codes.OK:
		return logger.WithLevel(successLevel)
	case codes.NotFound, codes.InvalidArgument, codes.AlreadyExists,
		codes.ResourceExhausted, codes.FailedPrecondition, codes.Canceled:
		return logger.Warn()
	default:
		return logger.Error()
	}
}
