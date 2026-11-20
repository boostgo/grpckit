package grpckit

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serverInterceptors builds the unary/stream chains in the order recovery -> trace ID -> logging ->
// user interceptors. Recovery is outermost so it also catches panics in the other interceptors.
func serverInterceptors(logger zerolog.Logger, o *ServerOptions) (
	[]grpc.UnaryServerInterceptor, []grpc.StreamServerInterceptor,
) {
	unary := make([]grpc.UnaryServerInterceptor, 0, 3+len(o.UnaryInterceptors))
	stream := make([]grpc.StreamServerInterceptor, 0, 3+len(o.StreamInterceptors))

	if o.Recovery {
		unary = append(unary, recoveryUnaryServerInterceptor(logger))
		stream = append(stream, recoveryStreamServerInterceptor(logger))
	}
	if o.TraceID {
		unary = append(unary, traceUnaryServerInterceptor())
		stream = append(stream, traceStreamServerInterceptor())
	}
	if o.Logging {
		unary = append(unary, loggingUnaryServerInterceptor(logger, o.SuccessLogLevel))
		stream = append(stream, loggingStreamServerInterceptor(logger, o.SuccessLogLevel))
	}

	unary = append(unary, o.UnaryInterceptors...)
	stream = append(stream, o.StreamInterceptors...)
	return unary, stream
}

func recoveryUnaryServerInterceptor(logger zerolog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(logger, info.FullMethod, r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}

func recoveryStreamServerInterceptor(logger zerolog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) (err error) {
		defer func() {
			if r := recover(); r != nil {
				logPanic(logger, info.FullMethod, r)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(srv, ss)
	}
}

func traceUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		if traceID := traceIDFromIncomingMD(ctx); traceID != "" {
			ctx = ContextWithTraceID(ctx, traceID)
		}
		return handler(ctx, req)
	}
}

func traceStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		if traceID := traceIDFromIncomingMD(ctx); traceID != "" {
			ctx = ContextWithTraceID(ctx, traceID)
			ss = &wrappedServerStream{ServerStream: ss, ctx: ctx}
		}
		return handler(srv, ss)
	}
}

func loggingUnaryServerInterceptor(logger zerolog.Logger, successLevel zerolog.Level) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)
		logEventByCode(logger, code, successLevel).
			Str("method", info.FullMethod).
			Str("code", code.String()).
			Dur("took", time.Since(start)).
			Str(TraceKey, TraceIDFromContext(ctx)).
			Err(err).
			Msg("grpc unary handled")
		return resp, err
	}
}

func loggingStreamServerInterceptor(logger zerolog.Logger, successLevel zerolog.Level) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
	) error {
		start := time.Now()
		err := handler(srv, ss)
		code := status.Code(err)
		logEventByCode(logger, code, successLevel).
			Str("method", info.FullMethod).
			Str("code", code.String()).
			Dur("took", time.Since(start)).
			Str(TraceKey, TraceIDFromContext(ss.Context())).
			Err(err).
			Msg("grpc stream handled")
		return err
	}
}

func logPanic(logger zerolog.Logger, method string, recovered any) {
	logger.Error().
		Str("method", method).
		Interface("panic", recovered).
		Bytes("stack", debug.Stack()).
		Msg("grpc handler panic recovered")
}
