package grpckit

import (
	"context"

	"google.golang.org/grpc"
)

// wrappedServerStream overrides Context() of a grpc.ServerStream so that an enriched context
// (carrying the trace ID) reaches the stream handler. All other methods delegate to the
// original stream.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context { return w.ctx }
