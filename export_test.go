package grpckit

import (
	"context"
	"net"
)

// SetListener replaces listener creation in Start (bufconn instead of TCP).
func SetListener(s *Server, lis net.Listener) {
	s.listen = func(context.Context, string) (net.Listener, error) { return lis, nil }
}

// TraceIDFromIncomingMD exposes traceIDFromIncomingMD to tests.
var TraceIDFromIncomingMD = traceIDFromIncomingMD

// ServiceConfig exposes serviceConfig to tests.
func ServiceConfig(o *ClientOptions) (string, error) { return serviceConfig(o) }

// SetMarshalJSON replaces json.Marshal in the service config builder and returns a restore func.
func SetMarshalJSON(f func(any) ([]byte, error)) (restore func()) {
	old := marshalJSON
	marshalJSON = f
	return func() { marshalJSON = old }
}
