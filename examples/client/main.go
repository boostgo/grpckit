// Command client dials a gRPC server with grpckit, propagates a trace ID and checks the server
// health. Start ./examples/server first.
//
//	go run ./examples/client -target localhost:9090
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/boostgo/grpckit"
	"github.com/rs/zerolog"
)

func main() {
	target := flag.String("target", "localhost:9090", "gRPC server target, e.g. dns:///svc.ns.svc.cluster.local:9090")
	flag.Parse()

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpckit.Dial(ctx, logger,
		grpckit.WithTarget(*target),
		grpckit.WithKeepAlive(30*time.Second),
		grpckit.WithPerCallTimeout(2*time.Second),
		grpckit.WithClientSuccessLogLevel(zerolog.InfoLevel),
		grpckit.WithBlock(), // wait until the connection is Ready
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("dial")
	}
	defer func() { _ = conn.Close() }()

	// The trace ID is forwarded to the server as "x-trace-id" metadata.
	ctx = grpckit.ContextWithTraceID(ctx, "example-trace-id")

	if err := grpckit.HealthCheck(ctx, conn, ""); err != nil {
		logger.Fatal().Err(err).Msg("health check")
	}
	logger.Info().Msg("server is healthy")

	// Use generated clients with the same connection:
	// client := greeterv1.NewGreeterClient(conn)
}
