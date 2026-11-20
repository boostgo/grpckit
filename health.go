package grpckit

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthCheck calls grpc.health.v1.Health/Check over conn. An empty service asks for the overall
// server status. It returns nil only when the status is SERVING, which makes it handy as a
// start-up connectivity probe right after Dial.
//
// Call it with a deadline in ctx. A transport error wraps ErrHealthCheck and preserves the gRPC
// status code (status.Code(err) yields codes.Unavailable for an unreachable server); any status
// other than SERVING yields ErrNotServing. The health method is never part of WithRetryMethods,
// so it is not retried, which also makes it suitable as a half-open probe for a circuit breaker.
//
//	if err := grpckit.HealthCheck(ctx, conn, ""); err != nil { log.Fatal(err) }
func HealthCheck(ctx context.Context, conn *grpc.ClientConn, service string) error {
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrHealthCheck, err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("%w: status: %s", ErrNotServing, resp.GetStatus())
	}
	return nil
}
