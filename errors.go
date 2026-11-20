package grpckit

import "errors"

var (
	// ErrAddrRequired is returned by NewServer when the listen address (WithAddr) is empty.
	ErrAddrRequired = errors.New("grpckit: server address is required")
	// ErrServerListen is returned by Server.Start when the TCP port cannot be bound.
	ErrServerListen = errors.New("grpckit: server listen failed")

	// ErrTargetRequired is returned by Dial when no target (WithTarget) is set.
	ErrTargetRequired = errors.New("grpckit: client target is required")
	// ErrClientDial is returned by Dial when the client connection cannot be created.
	ErrClientDial = errors.New("grpckit: client dial failed")
	// ErrInvalidRetry is returned by Dial for an invalid retry configuration (WithRetry /
	// WithRetryMethods): more than 5 attempts, a backoff below 1ms, or a method name that is not
	// in the "/<pkg.Service>/<Method>" format.
	ErrInvalidRetry = errors.New("grpckit: invalid retry configuration")
	// ErrClientNotReady is returned by Dial with WithBlock when the connection does not become
	// Ready before the context is done.
	ErrClientNotReady = errors.New("grpckit: client connection not ready")
	// ErrHealthCheck is returned by HealthCheck when the health RPC fails at the transport level.
	ErrHealthCheck = errors.New("grpckit: health check failed")
	// ErrNotServing is returned by HealthCheck when the server reports a status other than SERVING.
	ErrNotServing = errors.New("grpckit: server is not serving")
)
