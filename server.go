package grpckit

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// Server wraps a *grpc.Server together with its listener, built-in interceptors, health service
// and graceful shutdown logic. Create it with NewServer.
type Server struct {
	logger zerolog.Logger
	opts   *ServerOptions
	server *grpc.Server
	health *health.Server
	lis    net.Listener

	// listen is a test seam (bufconn); by default it is net.Listen("tcp", addr).
	listen func(ctx context.Context, addr string) (net.Listener, error)

	stopMu   sync.Mutex
	stopping bool
	stopped  chan struct{} // closed once the first Stop completes
}

// NewServer builds a *grpc.Server with the built-in interceptors. Register services through
// Registrar, start serving with Start (non-blocking) and shut down with Stop (graceful).
func NewServer(logger zerolog.Logger, opts ...ServerOption) (*Server, error) {
	options := newServerOptions()
	for _, o := range opts {
		o(options)
	}
	if err := options.validate(); err != nil {
		return nil, err
	}

	log := logger.With().Str("namespace", "grpckit.server").Logger()

	unary, stream := serverInterceptors(log, options)
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary...),
		grpc.ChainStreamInterceptor(stream...),
	}
	if options.MaxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(options.MaxRecvMsgSize))
	}
	if options.MaxSendMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(options.MaxSendMsgSize))
	}
	serverOpts = append(serverOpts, grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             options.KeepaliveMinTime,
		PermitWithoutStream: options.KeepalivePermitWithoutStream,
	}))
	if options.MaxConnectionAge > 0 {
		serverOpts = append(serverOpts, grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      options.MaxConnectionAge,
			MaxConnectionAgeGrace: options.MaxConnectionAgeGrace,
		}))
	}
	serverOpts = append(serverOpts, options.ServerOptions...)

	srv := grpc.NewServer(serverOpts...)

	var healthSrv *health.Server
	if options.Health {
		healthSrv = health.NewServer()
		healthpb.RegisterHealthServer(srv, healthSrv)
	}
	if options.Reflection {
		reflection.Register(srv)
	}

	return &Server{
		logger: log,
		opts:   options,
		server: srv,
		health: healthSrv,
		listen: func(ctx context.Context, addr string) (net.Listener, error) {
			return (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
		},
		stopped: make(chan struct{}),
	}, nil
}

// Registrar returns the grpc.ServiceRegistrar to register generated services on, for example
// greeterv1.RegisterGreeterServer(srv.Registrar(), impl).
func (s *Server) Registrar() grpc.ServiceRegistrar { return s.server }

// Health returns the health server for manual status management, or nil if health is disabled.
func (s *Server) Health() *health.Server { return s.health }

// Addr returns the actual listen address (useful with WithAddr("host:0")), or nil before Start.
func (s *Server) Addr() net.Addr {
	if s.lis == nil {
		return nil
	}
	return s.lis.Addr()
}

// Start binds the port synchronously (a bind error is returned immediately) and then runs Serve
// in the background, so it does not block. Errors from Serve after start-up are only logged
// (except for the regular shutdown error).
func (s *Server) Start(ctx context.Context) error {
	lis, err := s.listen(ctx, s.opts.Addr)
	if err != nil {
		return fmt.Errorf("%w: addr: %s: %w", ErrServerListen, s.opts.Addr, err)
	}
	s.lis = lis
	if s.health != nil {
		s.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	}

	go func() {
		s.logger.Info().Str("addr", s.opts.Addr).Msg("grpc server started")
		if serveErr := s.server.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			s.logger.Error().Err(serveErr).Msg("grpc server serve failed")
		}
	}()
	return nil
}

// Stop shuts the server down gracefully: the health status becomes NOT_SERVING, Stop waits for
// ShutdownPause (so clients and load balancers notice the status), then calls GracefulStop which
// waits for in-flight RPCs. If ctx is done first, the server is stopped forcefully. If ctx has no
// deadline, waiting is capped by StopTimeout, after which the server is also stopped forcefully.
//
// Stop is idempotent: a repeated call neither panics nor repeats the pause, it only waits for the
// first call to complete (or for its own ctx). Calling Stop before Start is safe and skips the
// pause. Stop always returns nil; the error result is reserved for future use.
func (s *Server) Stop(ctx context.Context) error {
	s.stopMu.Lock()
	first := !s.stopping
	s.stopping = true
	s.stopMu.Unlock()

	if !first {
		select {
		case <-s.stopped:
		case <-ctx.Done():
			s.server.Stop()
		}
		return nil
	}
	defer close(s.stopped)

	if s.health != nil {
		s.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	}

	if _, hasDeadline := ctx.Deadline(); !hasDeadline && s.opts.StopTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.StopTimeout)
		defer cancel()
	}

	if s.lis != nil && !pause(ctx, s.opts.ShutdownPause) {
		s.server.Stop()
		s.logger.Warn().Msg("grpc server force-stopped (deadline exceeded during shutdown pause)")
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info().Msg("grpc server stopped gracefully")
	case <-ctx.Done():
		s.server.Stop()
		s.logger.Warn().Msg("grpc server force-stopped (graceful deadline exceeded)")
	}
	return nil
}

// pause waits for d; it returns false if ctx is done first.
func pause(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
