// Command server runs a grpckit server with health and reflection enabled and shuts it down
// gracefully on SIGINT/SIGTERM.
//
//	go run ./examples/server
//	grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/boostgo/grpckit"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// An example custom interceptor: reject calls without a trace ID.
	requireTrace := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		if grpckit.TraceIDFromContext(ctx) == "" && os.Getenv("REQUIRE_TRACE") != "" {
			return nil, status.Error(codes.InvalidArgument, "missing x-trace-id")
		}
		return h(ctx, req)
	}

	srv, err := grpckit.NewServer(logger,
		grpckit.WithAddr(":9090"),
		grpckit.WithReflection(true), // development only
		grpckit.WithSuccessLogLevel(zerolog.InfoLevel),
		grpckit.WithMaxConnectionAge(30*time.Minute, time.Minute),
		grpckit.WithServerUnaryInterceptors(requireTrace),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("create server")
	}

	// Register your generated services here:
	// greeterv1.RegisterGreeterServer(srv.Registrar(), &greeter{})

	if err := srv.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("start server")
	}

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	_ = srv.Stop(shutdownCtx)
}
