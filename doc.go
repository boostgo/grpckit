// Package grpckit is a thin, batteries-included wrapper around google.golang.org/grpc.
//
// It lets a service expose or consume gRPC in about five lines per side, with a consistent set
// of interceptors (panic recovery, structured logging via zerolog, trace ID propagation) and
// Kubernetes-friendly client-side load balancing. The package contains no business logic and
// knows nothing about concrete contracts: register your generated *.pb.go services on the
// server and wrap the returned connection with generated clients.
//
// Server:
//
//	srv, _ := grpckit.NewServer(logger, grpckit.WithAddr(":9090"), grpckit.WithReflection(false))
//	greeterv1.RegisterGreeterServer(srv.Registrar(), impl)
//	_ = srv.Start(ctx)          // non-blocking
//	defer srv.Stop(ctx)         // graceful
//
// Client:
//
//	conn, _ := grpckit.Dial(ctx, logger,
//	    grpckit.WithTarget("dns:///greeter.default.svc.cluster.local:9090"),
//	    grpckit.WithRoundRobin(true), grpckit.WithPerCallTimeout(2*time.Second),
//	    grpckit.WithClientMaxRecvMsgSize(16<<20)) // the grpc default is 4 MiB on receive
//	defer conn.Close()
//	client := greeterv1.NewGreeterClient(conn)
//
// Tracing: the trace ID travels in the gRPC metadata key "x-trace-id" and is stored in the
// context.Context under TraceKey ("trace_id"), the same key used by the sibling libraries
// github.com/boostgo/httpkit and github.com/boostgo/kafkakit, so log lines can be correlated
// across HTTP, Kafka and gRPC hops.
package grpckit
