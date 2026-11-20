package grpckit

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip" // registers the gzip compressor (including decompression) with grpc
	"google.golang.org/grpc/keepalive"
)

// Dial creates a client connection to a gRPC server. The connection multiplexes all RPCs, so
// create one per process and reuse it rather than dialing per call. The connection is lazy by
// default; with WithBlock, Dial waits for the Ready state bounded by ctx.
//
// The returned *grpc.ClientConn can be passed to any generated client constructor.
func Dial(ctx context.Context, logger zerolog.Logger, opts ...ClientOption) (*grpc.ClientConn, error) {
	options := newClientOptions()
	for _, o := range opts {
		o(options)
	}
	if err := options.validate(); err != nil {
		return nil, err
	}

	log := logger.With().Str("namespace", "grpckit.client").Logger()

	if options.MaxRetries >= 2 && len(options.RetryMethods) == 0 {
		log.Warn().Msg("WithRetry without WithRetryMethods: retries are disabled")
	}

	dialOpts, err := dialOptions(log, options)
	if err != nil {
		return nil, fmt.Errorf("%w: target: %s: %w", ErrClientDial, options.Target, err)
	}

	conn, err := grpc.NewClient(options.Target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("%w: target: %s: %w", ErrClientDial, options.Target, err)
	}

	if options.Block {
		if err := waitReady(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	log.Info().Str("target", options.Target).Bool("round_robin", options.RoundRobin).Msg("grpc client created")
	return conn, nil
}

func dialOptions(logger zerolog.Logger, o *ClientOptions) ([]grpc.DialOption, error) {
	dialOpts := make([]grpc.DialOption, 0, 7)

	if o.TLSConfig != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(o.TLSConfig)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	cfg, err := serviceConfig(o)
	if err != nil {
		return nil, err
	}
	if cfg != "" {
		dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(cfg))
	}

	if o.KeepAlive > 0 {
		dialOpts = append(dialOpts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                o.KeepAlive,
			Timeout:             o.KeepAliveTimeout,
			PermitWithoutStream: true,
		}))
	}

	if callOpts := defaultCallOptions(o); len(callOpts) > 0 {
		dialOpts = append(dialOpts, grpc.WithDefaultCallOptions(callOpts...))
	}

	unary, stream := clientInterceptors(logger, o)
	dialOpts = append(dialOpts,
		grpc.WithChainUnaryInterceptor(unary...),
		grpc.WithChainStreamInterceptor(stream...),
	)
	dialOpts = append(dialOpts, o.DialOptions...)
	return dialOpts, nil
}

// defaultCallOptions collects the options applied to EVERY call on the connection: message size
// limits and the compressor. If nothing is set it returns an empty slice, grpc.WithDefaultCallOptions
// is not added and the grpc defaults apply (4 MiB on receive, no compression). Options passed to an
// individual call override these; the interceptors (trace/timeout/logging) pass them through as is.
func defaultCallOptions(o *ClientOptions) []grpc.CallOption {
	callOpts := make([]grpc.CallOption, 0, 3)
	if o.MaxRecvMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallRecvMsgSize(o.MaxRecvMsgSize))
	}
	if o.MaxSendMsgSize > 0 {
		callOpts = append(callOpts, grpc.MaxCallSendMsgSize(o.MaxSendMsgSize))
	}
	if o.Gzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	return callOpts
}

// marshalJSON is a test seam for service config build errors.
var marshalJSON = json.Marshal

// serviceConfig builds the JSON default service config: round_robin plus a retryPolicy on
// UNAVAILABLE for the methods in RetryMethods only. It returns an empty string when neither load
// balancing nor retries are configured.
func serviceConfig(o *ClientOptions) (string, error) {
	cfg := map[string]any{}
	if o.RoundRobin {
		cfg["loadBalancingConfig"] = []any{map[string]any{"round_robin": map[string]any{}}}
	}
	if o.retryEnabled() {
		names := make([]any, 0, len(o.RetryMethods))
		seen := make(map[string]struct{}, len(o.RetryMethods))
		for _, m := range o.RetryMethods {
			if _, dup := seen[m]; dup {
				continue
			}
			seen[m] = struct{}{}
			service, method, err := splitMethod(m)
			if err != nil {
				return "", err
			}
			names = append(names, map[string]any{"service": service, "method": method})
		}
		cfg["methodConfig"] = []any{map[string]any{
			"name": names,
			"retryPolicy": map[string]any{
				"maxAttempts":          o.MaxRetries,
				"initialBackoff":       durationString(o.RetryBackoff),
				"maxBackoff":           durationString(o.RetryBackoff * 10),
				"backoffMultiplier":    2.0,
				"retryableStatusCodes": []any{"UNAVAILABLE"},
			},
		}}
	}
	if len(cfg) == 0 {
		return "", nil
	}
	raw, err := marshalJSON(cfg)
	if err != nil {
		return "", fmt.Errorf("service config: %w", err)
	}
	return string(raw), nil
}

// waitReady blocks until the connection becomes Ready or ctx is done.
func waitReady(ctx context.Context, conn *grpc.ClientConn) error {
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}
		if !conn.WaitForStateChange(ctx, state) {
			return fmt.Errorf("%w: target: %s: %w", ErrClientNotReady, conn.Target(), ctx.Err())
		}
	}
}

func durationString(d time.Duration) string {
	return fmt.Sprintf("%.3fs", d.Seconds())
}
