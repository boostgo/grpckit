package grpckit

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// clientInterceptors builds the unary/stream chains in the order trace ID -> timeout -> logging ->
// user interceptors.
func clientInterceptors(logger zerolog.Logger, o *ClientOptions) (
	[]grpc.UnaryClientInterceptor, []grpc.StreamClientInterceptor,
) {
	unary := make([]grpc.UnaryClientInterceptor, 0, 3+len(o.UnaryInterceptors))
	stream := make([]grpc.StreamClientInterceptor, 0, 2+len(o.StreamInterceptors))

	if o.TraceInjection {
		unary = append(unary, traceUnaryClientInterceptor())
		stream = append(stream, traceStreamClientInterceptor())
	}
	if o.PerCallTimeout > 0 {
		unary = append(unary, timeoutUnaryClientInterceptor(o.PerCallTimeout))
	}
	if o.Logging {
		unary = append(unary, loggingUnaryClientInterceptor(logger, o.SuccessLogLevel))
	}

	unary = append(unary, o.UnaryInterceptors...)
	stream = append(stream, o.StreamInterceptors...)
	return unary, stream
}

func traceUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		return invoker(outgoingTraceContext(ctx), method, req, reply, cc, opts...)
	}
}

func traceStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn,
		method string, streamer grpc.Streamer, opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		return streamer(outgoingTraceContext(ctx), desc, cc, method, opts...)
	}
}

// outgoingTraceContext adds the trace ID from ctx (if any) to the outgoing metadata.
func outgoingTraceContext(ctx context.Context) context.Context {
	traceID := TraceIDFromContext(ctx)
	if traceID == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, TraceMetadataKey, traceID)
}

func timeoutUnaryClientInterceptor(timeout time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func loggingUnaryClientInterceptor(logger zerolog.Logger, successLevel zerolog.Level) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		start := time.Now()
		err := invoker(ctx, method, req, reply, cc, opts...)
		code := status.Code(err)
		logEventByCode(logger, code, successLevel).
			Str("method", method).
			Str("code", code.String()).
			Dur("took", time.Since(start)).
			Str(TraceKey, TraceIDFromContext(ctx)).
			Err(err).
			Msg("grpc unary call")
		return err
	}
}
