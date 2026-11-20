package grpckit_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/boostgo/grpckit"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// startServer starts a server on an ephemeral port and returns its address and a stop func.
func startServer(t *testing.T, opts ...grpckit.ServerOption) (string, func()) {
	t.Helper()
	logger := zerolog.New(io.Discard)
	opts = append([]grpckit.ServerOption{grpckit.WithAddr("127.0.0.1:0")}, opts...)
	srv, err := grpckit.NewServer(logger, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return srv.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}
}

func dial(t *testing.T, target string, opts ...grpckit.ClientOption) *grpc.ClientConn {
	t.Helper()
	logger := zerolog.New(io.Discard)
	opts = append([]grpckit.ClientOption{grpckit.WithTarget(target)}, opts...)
	conn, err := grpckit.Dial(context.Background(), logger, opts...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return conn
}

// TestHealthRoundTrip: the server starts, the client connects and health answers SERVING.
// It exercises server/client construction, interceptors and the transport end to end.
func TestHealthRoundTrip(t *testing.T) {
	target, stop := startServer(t)
	defer stop()

	conn := dial(t, target, grpckit.WithPerCallTimeout(2*time.Second))
	defer conn.Close()

	resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", resp.GetStatus())
	}
}

// TestTracePropagation: the trace ID from the client ctx reaches the server ctx via metadata.
func TestTracePropagation(t *testing.T) {
	const wantTrace = "trace-abc-123"
	got := make(chan string, 1)

	// This server interceptor reads the trace ID already put into ctx by the built-in trace interceptor.
	capture := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		got <- grpckit.TraceIDFromContext(ctx)
		return h(ctx, req)
	}

	target, stop := startServer(t, grpckit.WithServerUnaryInterceptors(capture))
	defer stop()

	conn := dial(t, target)
	defer conn.Close()

	ctx := grpckit.ContextWithTraceID(context.Background(), wantTrace)
	if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatalf("Check: %v", err)
	}

	select {
	case g := <-got:
		if g != wantTrace {
			t.Fatalf("server trace_id = %q, want %q", g, wantTrace)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server interceptor not invoked")
	}
}

// TestHealthCheck: the start-up probe returns nil for SERVING.
func TestHealthCheck(t *testing.T) {
	target, stop := startServer(t)
	defer stop()

	conn := dial(t, target)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := grpckit.HealthCheck(ctx, conn, ""); err != nil {
		t.Fatalf("HealthCheck (serving): %v", err)
	}
}

func TestValidation(t *testing.T) {
	logger := zerolog.New(io.Discard)
	if _, err := grpckit.Dial(context.Background(), logger); err == nil {
		t.Fatal("Dial without target: want error, got nil")
	}
	if _, err := grpckit.NewServer(logger, grpckit.WithAddr("")); err == nil {
		t.Fatal("NewServer with empty addr: want error, got nil")
	}
}

// --- message size limits -----------------------------------------------------------------------

const (
	bigEchoMethod   = "/grpckit.test.Big/Echo"
	bigStreamMethod = "/grpckit.test.Big/Stream"

	bigPayload = 5 << 20  // 5 MiB, well above the 4 MiB grpc default
	bigLimit   = 16 << 20 // 16 MiB, the raised limit for server and client
)

// bigServiceDesc is a minimal hand-built echo service (no protoc) for size limit tests. It reuses
// healthpb.HealthCheckRequest as the message and puts a payload of the desired size into its string
// field. Unary Echo and server-stream Stream return the incoming message as is, so one request
// exercises both the send and the receive limit.
var bigServiceDesc = grpc.ServiceDesc{
	ServiceName: "grpckit.test.Big",
	HandlerType: (*any)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Echo",
		Handler: func(srv any, ctx context.Context, dec func(any) error,
			interceptor grpc.UnaryServerInterceptor,
		) (any, error) {
			in := new(healthpb.HealthCheckRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			echo := func(_ context.Context, req any) (any, error) { return req, nil }
			if interceptor == nil {
				return echo(ctx, in)
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: bigEchoMethod}, echo)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName:    "Stream",
		ServerStreams: true,
		Handler: func(_ any, stream grpc.ServerStream) error {
			in := new(healthpb.HealthCheckRequest)
			if err := stream.RecvMsg(in); err != nil {
				return err
			}
			return stream.SendMsg(in)
		},
	}},
}

// startBigServer starts a server with the echo service and raised limits (so the client hits its own).
func startBigServer(t *testing.T, opts ...grpckit.ServerOption) (string, func()) {
	t.Helper()
	logger := zerolog.New(io.Discard)
	opts = append([]grpckit.ServerOption{
		grpckit.WithAddr("127.0.0.1:0"),
		grpckit.WithMaxRecvMsgSize(bigLimit),
		grpckit.WithMaxSendMsgSize(bigLimit),
	}, opts...)
	srv, err := grpckit.NewServer(logger, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Registrar().RegisterService(&bigServiceDesc, struct{}{})
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return srv.Addr().String(), func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
	}
}

func bigRequest() *healthpb.HealthCheckRequest {
	return &healthpb.HealthCheckRequest{Service: strings.Repeat("a", bigPayload)}
}

// echoUnary sends a large unary request and returns the call error.
func echoUnary(ctx context.Context, conn *grpc.ClientConn) error {
	return conn.Invoke(ctx, bigEchoMethod, bigRequest(), new(healthpb.HealthCheckRequest))
}

// echoStream sends a large message on a stream and reads the same one back.
func echoStream(ctx context.Context, conn *grpc.ClientConn) error {
	desc := &grpc.StreamDesc{StreamName: "Stream", ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, bigStreamMethod)
	if err != nil {
		return err
	}
	if err := stream.SendMsg(bigRequest()); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}
	return stream.RecvMsg(new(healthpb.HealthCheckRequest))
}

// TestClientMaxRecvMsgSizeUnary: a 5 MiB response fails with the default client limit (4 MiB) and
// succeeds with WithClientMaxRecvMsgSize. The server limit is raised, so the client is what fails.
func TestClientMaxRecvMsgSizeUnary(t *testing.T) {
	target, stop := startBigServer(t)
	defer stop()

	t.Run("default limit fails", func(t *testing.T) {
		conn := dial(t, target)
		defer conn.Close()

		err := echoUnary(context.Background(), conn)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (err: %v), want ResourceExhausted", status.Code(err), err)
		}
	})

	t.Run("raised limit passes", func(t *testing.T) {
		conn := dial(t, target, grpckit.WithClientMaxRecvMsgSize(bigLimit))
		defer conn.Close()

		if err := echoUnary(context.Background(), conn); err != nil {
			t.Fatalf("Invoke with raised recv limit: %v", err)
		}
	})

	t.Run("zero keeps grpc default", func(t *testing.T) {
		conn := dial(t, target, grpckit.WithClientMaxRecvMsgSize(0))
		defer conn.Close()

		err := echoUnary(context.Background(), conn)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (err: %v), want ResourceExhausted", status.Code(err), err)
		}
	})
}

// TestClientMaxRecvMsgSizeStream: the same for streams; connection-wide call options also apply
// to streaming calls (for example, a snapshot stream sending large pages).
func TestClientMaxRecvMsgSizeStream(t *testing.T) {
	target, stop := startBigServer(t)
	defer stop()

	t.Run("default limit fails", func(t *testing.T) {
		conn := dial(t, target)
		defer conn.Close()

		err := echoStream(context.Background(), conn)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (err: %v), want ResourceExhausted", status.Code(err), err)
		}
	})

	t.Run("raised limit passes", func(t *testing.T) {
		conn := dial(t, target,
			grpckit.WithClientMaxRecvMsgSize(bigLimit),
			grpckit.WithTraceIDInjection(true),
			grpckit.WithRetry(3, 50*time.Millisecond), // retries + streams do not conflict with call options
		)
		defer conn.Close()

		if err := echoStream(context.Background(), conn); err != nil {
			t.Fatalf("stream with raised recv limit: %v", err)
		}
	})
}

// TestClientMaxSendMsgSize: an outgoing message is rejected by the configured limit; without the
// option the grpc default (math.MaxInt32) applies, so 5 MiB goes through.
func TestClientMaxSendMsgSize(t *testing.T) {
	target, stop := startBigServer(t)
	defer stop()

	t.Run("small limit fails", func(t *testing.T) {
		conn := dial(t, target,
			grpckit.WithClientMaxRecvMsgSize(bigLimit),
			grpckit.WithClientMaxSendMsgSize(1<<20),
		)
		defer conn.Close()

		err := echoUnary(context.Background(), conn)
		if status.Code(err) != codes.ResourceExhausted {
			t.Fatalf("code = %v (err: %v), want ResourceExhausted", status.Code(err), err)
		}
	})

	t.Run("unset keeps grpc default", func(t *testing.T) {
		conn := dial(t, target, grpckit.WithClientMaxRecvMsgSize(bigLimit))
		defer conn.Close()

		if err := echoUnary(context.Background(), conn); err != nil {
			t.Fatalf("Invoke without send limit: %v", err)
		}
	})
}

// TestClientGzip: WithGzip(true) really compresses the outgoing message (5 MiB of uniform text
// shrinks to a few kilobytes), WithGzip(false)/the default does not. The on-the-wire size is
// captured by a server stats.Handler.
func TestClientGzip(t *testing.T) {
	wire := make(chan *stats.InPayload, 8)
	handler := &payloadStats{wire: wire}

	target, stop := startBigServer(t, grpckit.WithServerOptions(grpc.StatsHandler(handler)))
	defer stop()

	recvWire := func(t *testing.T) *stats.InPayload {
		t.Helper()
		select {
		case in := <-wire:
			return in
		case <-time.After(5 * time.Second):
			t.Fatal("no InPayload stats from server")
			return nil
		}
	}

	t.Run("enabled compresses", func(t *testing.T) {
		conn := dial(t, target, grpckit.WithGzip(true), grpckit.WithClientMaxRecvMsgSize(bigLimit))
		defer conn.Close()

		if err := echoUnary(context.Background(), conn); err != nil {
			t.Fatalf("Invoke with gzip: %v", err)
		}
		in := recvWire(t)
		if in.CompressedLength >= in.Length/2 {
			t.Fatalf("compressed = %d, raw = %d: payload does not look gzipped", in.CompressedLength, in.Length)
		}
	})

	t.Run("disabled by default", func(t *testing.T) {
		conn := dial(t, target, grpckit.WithClientMaxRecvMsgSize(bigLimit))
		defer conn.Close()

		if err := echoUnary(context.Background(), conn); err != nil {
			t.Fatalf("Invoke without gzip: %v", err)
		}
		in := recvWire(t)
		if in.CompressedLength < in.Length {
			t.Fatalf("compressed = %d, raw = %d: payload unexpectedly compressed", in.CompressedLength, in.Length)
		}
	})
}

// payloadStats is a minimal stats.Handler that exposes inbound payload statistics.
type payloadStats struct {
	wire chan *stats.InPayload
}

func (p *payloadStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }

func (p *payloadStats) HandleRPC(_ context.Context, s stats.RPCStats) {
	if in, ok := s.(*stats.InPayload); ok {
		select {
		case p.wire <- in:
		default:
		}
	}
}

func (p *payloadStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }

func (p *payloadStats) HandleConn(context.Context, stats.ConnStats) {}
