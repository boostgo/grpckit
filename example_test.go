package grpckit_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/boostgo/grpckit"
	"github.com/rs/zerolog"
)

// ExampleNewServer starts a server with connection rotation and a short shutdown pause, then
// stops it gracefully.
func ExampleNewServer() {
	logger := zerolog.New(os.Stderr)

	srv, err := grpckit.NewServer(logger,
		grpckit.WithAddr("127.0.0.1:0"),
		grpckit.WithMaxConnectionAge(30*time.Minute, time.Minute), // rotate connections for round_robin
		grpckit.WithShutdownPause(0),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	// greeterv1.RegisterGreeterServer(srv.Registrar(), greeterImpl)
	if err := srv.Start(context.Background()); err != nil { // non-blocking
		fmt.Println(err)
		return
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = srv.Stop(shutdownCtx)
	fmt.Println("stopped")
	// Output: stopped
}

// ExampleDial creates a load-balanced client connection with retries for an idempotent method.
func ExampleDial() {
	logger := zerolog.Nop()

	conn, err := grpckit.Dial(context.Background(), logger,
		grpckit.WithTarget("dns:///greeter.default.svc.cluster.local:9090"),
		grpckit.WithKeepAlive(30*time.Second),
		grpckit.WithPerCallTimeout(2*time.Second),
		grpckit.WithRetry(3, 100*time.Millisecond),
		// Idempotent methods only!
		grpckit.WithRetryMethods("/greeter.v1.Greeter/GetGreeting"),
	)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = conn.Close() }()
	// client := greeterv1.NewGreeterClient(conn)
	fmt.Println("connection created lazily")
	// Output: connection created lazily
}

// ExampleDial_invalidRetry shows that misconfigured retries are rejected at dial time.
func ExampleDial_invalidRetry() {
	_, err := grpckit.Dial(context.Background(), zerolog.Nop(),
		grpckit.WithTarget("localhost:9090"),
		grpckit.WithRetryMethods("GetGreeting"), // missing "/<pkg.Service>/" prefix
	)
	fmt.Println(errors.Is(err, grpckit.ErrInvalidRetry))
	// Output: true
}

// ExampleHealthCheck starts a server and probes it with HealthCheck right after Dial.
func ExampleHealthCheck() {
	srv, err := grpckit.NewServer(zerolog.Nop(), grpckit.WithAddr("127.0.0.1:0"), grpckit.WithShutdownPause(0))
	if err != nil {
		fmt.Println(err)
		return
	}
	if err := srv.Start(context.Background()); err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = srv.Stop(context.Background()) }()

	conn, err := grpckit.Dial(context.Background(), zerolog.Nop(), grpckit.WithTarget(srv.Addr().String()))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := grpckit.HealthCheck(ctx, conn, ""); err != nil {
		fmt.Println("server is not healthy:", err)
		return
	}
	fmt.Println("server is healthy")
	// Output: server is healthy
}

// ExampleContextWithTraceID attaches a trace ID that the client interceptor forwards as the
// "x-trace-id" metadata entry.
func ExampleContextWithTraceID() {
	ctx := grpckit.ContextWithTraceID(context.Background(), "4bf92f3577b34da6")
	fmt.Println(grpckit.TraceIDFromContext(ctx))
	// Output: 4bf92f3577b34da6
}
