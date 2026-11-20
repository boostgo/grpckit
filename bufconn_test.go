package grpckit_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boostgo/grpckit"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufTarget = "passthrough:///bufnet"

// syncBuffer is a goroutine-safe buffer for zerolog output.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// entries parses the captured JSON log lines (one line per entry).
func (b *syncBuffer) entries(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// bufServer starts a server on bufconn; stop and listener close are registered with t.Cleanup.
func bufServer(t *testing.T, logger zerolog.Logger, opts ...grpckit.ServerOption) (*grpckit.Server, *bufconn.Listener) {
	t.Helper()
	// The Stop pause is disabled by default in tests (graceful stop tests set their own).
	opts = append([]grpckit.ServerOption{grpckit.WithShutdownPause(0)}, opts...)
	srv, err := grpckit.NewServer(logger, opts...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	grpckit.SetListener(srv, lis)
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		_ = lis.Close()
	})
	return srv, lis
}

func bufDial(t *testing.T, lis *bufconn.Listener, logger zerolog.Logger, opts ...grpckit.ClientOption) *grpc.ClientConn {
	t.Helper()
	opts = append([]grpckit.ClientOption{
		grpckit.WithTarget(bufTarget),
		grpckit.WithDialOptions(grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		})),
	}, opts...)
	conn, err := grpckit.Dial(context.Background(), logger, opts...)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func check(ctx context.Context, conn *grpc.ClientConn) error {
	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func waitFor(t *testing.T, ctx context.Context, conn *grpc.ClientConn, want connectivity.State) {
	t.Helper()
	for conn.GetState() != want {
		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			t.Fatalf("state never became %v (last %v): %v", want, conn.GetState(), ctx.Err())
		}
	}
}

// --- test service ------------------------------------------------------------------------------

// testService is a hand-built grpckit.test.Flaky service using the health messages:
// Listed/Unlisted are unary, always return UNAVAILABLE and count calls; Ok is a successful unary;
// Stream is a successful server stream.
type testService struct {
	listed, unlisted atomic.Int32
}

func unaryDesc(name string, fn func(context.Context) error) grpc.MethodDesc {
	full := "/grpckit.test.Flaky/" + name
	return grpc.MethodDesc{
		MethodName: name,
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor,
		) (any, error) {
			in := new(healthpb.HealthCheckRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			h := func(ctx context.Context, _ any) (any, error) {
				return &healthpb.HealthCheckResponse{}, fn(ctx)
			}
			if interceptor == nil {
				return h(ctx, in)
			}
			return interceptor(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: full}, h)
		},
	}
}

func (s *testService) register(reg grpc.ServiceRegistrar, onOK func(context.Context)) {
	unavailable := status.Error(codes.Unavailable, "boom")
	reg.RegisterService(&grpc.ServiceDesc{
		ServiceName: "grpckit.test.Flaky",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			unaryDesc("Listed", func(context.Context) error { s.listed.Add(1); return unavailable }),
			unaryDesc("Unlisted", func(context.Context) error { s.unlisted.Add(1); return unavailable }),
			unaryDesc("Ok", func(ctx context.Context) error {
				if onOK != nil {
					onOK(ctx)
				}
				return nil
			}),
		},
		Streams: []grpc.StreamDesc{{
			StreamName:    "Stream",
			ServerStreams: true,
			Handler: func(_ any, st grpc.ServerStream) error {
				return st.RecvMsg(new(healthpb.HealthCheckRequest))
			},
		}},
	}, s)
}

func invoke(ctx context.Context, conn *grpc.ClientConn, method string) error {
	return conn.Invoke(ctx, "/grpckit.test.Flaky/"+method, &healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
}

// runStream opens the Stream server stream, sends a request and reads it to the end.
func runStream(ctx context.Context, conn *grpc.ClientConn) error {
	st, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/grpckit.test.Flaky/Stream")
	if err != nil {
		return err
	}
	if err := st.SendMsg(&healthpb.HealthCheckRequest{}); err != nil {
		return err
	}
	if err := st.CloseSend(); err != nil {
		return err
	}
	if err := st.RecvMsg(new(healthpb.HealthCheckResponse)); !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func ctxTimeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// --- keepalive and connection age ------------------------------------------------------------

// idleStaysReady waits 3+ keepalive periods (10s) while idle and reports whether the connection
// stayed Ready.
func idleStaysReady(t *testing.T, serverOpts ...grpckit.ServerOption) bool {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping 35s keepalive test in -short mode")
	}
	logger := zerolog.New(io.Discard)
	_, lis := bufServer(t, logger, serverOpts...)
	conn := bufDial(t, lis, logger, grpckit.WithKeepAlive(10*time.Second))
	if err := check(ctxTimeout(t, 5*time.Second), conn); err != nil {
		t.Fatalf("Check: %v", err)
	}
	waitFor(t, ctxTimeout(t, 5*time.Second), conn, connectivity.Ready)

	// If the state changed within 35s, the server closed the connection (GOAWAY too_many_pings).
	changed := conn.WaitForStateChange(ctxTimeout(t, 35*time.Second), connectivity.Ready)
	return !changed
}

func TestServer_IdleKeepaliveNoGoaway(t *testing.T) {
	t.Parallel()
	if !idleStaysReady(t) {
		t.Fatal("server with the default keepalive policy closed the connection (GOAWAY too_many_pings)")
	}
}

// WithKeepaliveEnforcement overrides the default: a strict policy closes the connection on the same
// pings (which also proves the idle keepalive test is able to detect GOAWAY).
func TestServer_KeepaliveEnforcementOverride(t *testing.T) {
	t.Parallel()
	if idleStaysReady(t, grpckit.WithKeepaliveEnforcement(time.Hour, false)) {
		t.Fatal("strict policy had no effect: the connection survived the pings")
	}
}

func TestServer_MaxConnectionAgeReconnect(t *testing.T) {
	t.Parallel()
	conns := make(chan struct{}, 16)
	sh := connBeginHandler{ch: conns}
	logger := zerolog.New(io.Discard)
	_, lis := bufServer(t, logger,
		grpckit.WithMaxConnectionAge(200*time.Millisecond, time.Second),
		grpckit.WithServerOptions(grpc.StatsHandler(sh)),
	)
	conn := bufDial(t, lis, logger)
	ctx := ctxTimeout(t, 20*time.Second)

	if err := check(ctx, conn); err != nil {
		t.Fatalf("first Check: %v", err)
	}
	<-conns
	waitFor(t, ctx, conn, connectivity.Ready)
	// GOAWAY due to connection age: the connection leaves Ready.
	if !conn.WaitForStateChange(ctx, connectivity.Ready) {
		t.Fatalf("connection did not receive GOAWAY: %v", ctx.Err())
	}
	// The client reconnects and calls succeed.
	if err := check(ctx, conn); err != nil {
		t.Fatalf("Check after GOAWAY: %v", err)
	}
	select {
	case <-conns:
	case <-ctx.Done():
		t.Fatal("no new connection was established")
	}
}

type connBeginHandler struct{ ch chan struct{} }

func (connBeginHandler) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (connBeginHandler) HandleRPC(context.Context, stats.RPCStats)                       {}
func (connBeginHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h connBeginHandler) HandleConn(_ context.Context, s stats.ConnStats) {
	if _, ok := s.(*stats.ConnBegin); ok {
		select {
		case h.ch <- struct{}{}:
		default:
		}
	}
}

// --- retries ------------------------------------------------------------

func retryEnv(t *testing.T, opts ...grpckit.ClientOption) (*testService, *grpc.ClientConn) {
	t.Helper()
	logger := zerolog.New(io.Discard)
	srv, lis := bufServer(t, logger)
	svc := &testService{}
	svc.register(srv.Registrar(), nil)
	return svc, bufDial(t, lis, logger, opts...)
}

func TestDial_RetryOnlyListedMethods(t *testing.T) {
	svc, conn := retryEnv(t,
		grpckit.WithRetry(3, time.Millisecond),
		grpckit.WithRetryMethods("/grpckit.test.Flaky/Listed", "/grpckit.test.Flaky/Listed"),
	)
	ctx := ctxTimeout(t, 10*time.Second)

	if code := status.Code(invoke(ctx, conn, "Listed")); code != codes.Unavailable {
		t.Fatalf("Listed code = %v", code)
	}
	if got := svc.listed.Load(); got != 3 {
		t.Fatalf("listed attempts = %d, want 3", got)
	}
	if code := status.Code(invoke(ctx, conn, "Unlisted")); code != codes.Unavailable {
		t.Fatalf("Unlisted code = %v", code)
	}
	if got := svc.unlisted.Load(); got != 1 {
		t.Fatalf("unlisted attempts = %d, want 1", got)
	}
}

func TestDial_RetryWithoutMethodsNoRetry(t *testing.T) {
	var logs syncBuffer
	logger := zerolog.New(&logs)
	srv, lis := bufServer(t, zerolog.New(io.Discard))
	svc := &testService{}
	svc.register(srv.Registrar(), nil)
	conn := bufDial(t, lis, logger, grpckit.WithRetry(3, time.Millisecond))

	if code := status.Code(invoke(ctxTimeout(t, 10*time.Second), conn, "Listed")); code != codes.Unavailable {
		t.Fatalf("code = %v", code)
	}
	if got := svc.listed.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
	warned := false
	for _, e := range logs.entries(t) {
		if e["level"] == "warn" && strings.Contains(e["message"].(string), "WithRetry without WithRetryMethods") {
			warned = true
		}
	}
	if !warned {
		t.Fatal("missing warning about WithRetry without WithRetryMethods")
	}
}

func TestDial_InvalidMethodFormat(t *testing.T) {
	for _, m := range []string{"", "Listed", "/svc", "/svc/", "//M", "/a/b/c", "svc/M"} {
		_, err := grpckit.Dial(context.Background(), zerolog.New(io.Discard),
			grpckit.WithTarget(bufTarget), grpckit.WithRetryMethods(m))
		if !errors.Is(err, grpckit.ErrInvalidRetry) {
			t.Errorf("method %q: err = %v, want ErrInvalidRetry", m, err)
		}
	}
}

// --- retry validation and service config ------------------------------------------------------------

func TestDial_InvalidRetry(t *testing.T) {
	m := grpckit.WithRetryMethods("/svc.S/M")
	cases := map[string][]grpckit.ClientOption{
		"attempts>5":   {grpckit.WithRetry(6, time.Millisecond), m},
		"backoff<1ms":  {grpckit.WithRetry(3, 500*time.Microsecond), m},
		"both too bad": {grpckit.WithRetry(6, 500*time.Microsecond), m},
	}
	for name, opts := range cases {
		_, err := grpckit.Dial(context.Background(), zerolog.New(io.Discard),
			append([]grpckit.ClientOption{grpckit.WithTarget(bufTarget)}, opts...)...)
		if !errors.Is(err, grpckit.ErrInvalidRetry) {
			t.Errorf("%s: err = %v, want ErrInvalidRetry", name, err)
		}
	}
	// Retries disabled (no methods or < 2 attempts) is not an error, even with "bad" values.
	for name, opts := range map[string][]grpckit.ClientOption{
		"no methods":  {grpckit.WithRetry(9, 500*time.Microsecond)},
		"one attempt": {grpckit.WithRetry(1, time.Millisecond), m},
		"negative":    {grpckit.WithRetry(-3, time.Millisecond), m},
	} {
		conn, err := grpckit.Dial(context.Background(), zerolog.New(io.Discard),
			append([]grpckit.ClientOption{grpckit.WithTarget(bufTarget)}, opts...)...)
		if err != nil {
			t.Errorf("%s: unexpected error %v", name, err)
			continue
		}
		_ = conn.Close()
	}
}

func TestDial_ServiceConfigMarshalError(t *testing.T) {
	boom := errors.New("boom")
	restore := grpckit.SetMarshalJSON(func(any) ([]byte, error) { return nil, boom })
	defer restore()

	opts := &grpckit.ClientOptions{RoundRobin: true}
	if _, err := grpckit.ServiceConfig(opts); !errors.Is(err, boom) {
		t.Fatalf("serviceConfig err = %v, want boom", err)
	}
	_, err := grpckit.Dial(context.Background(), zerolog.New(io.Discard), grpckit.WithTarget(bufTarget))
	if !errors.Is(err, grpckit.ErrClientDial) || !errors.Is(err, boom) {
		t.Fatalf("Dial err = %v, want ErrClientDial+boom", err)
	}
}

// --- graceful stop ------------------------------------------------------------

// watchStream opens health.Watch (a server stream that stays open until cancelled) and reads the
// first message.
func watchStream(t *testing.T, conn *grpc.ClientConn) (recv func() (healthpb.HealthCheckResponse_ServingStatus, error), cancel context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	recv = func() (healthpb.HealthCheckResponse_ServingStatus, error) {
		r, err := st.Recv()
		return r.GetStatus(), err
	}
	if s, err := recv(); err != nil || s != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("first Watch msg = %v, %v", s, err)
	}
	return recv, cancel
}

func TestServer_StopCeilingWithoutDeadline(t *testing.T) {
	logger := zerolog.New(io.Discard)
	const ceiling = 300 * time.Millisecond
	srv, lis := bufServer(t, logger, grpckit.WithShutdownPause(0), grpckit.WithStopTimeout(ceiling))
	conn := bufDial(t, lis, logger)
	watchStream(t, conn) // the open stream keeps GracefulStop from completing

	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- srv.Stop(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stop hung with a ctx without deadline")
	}
	if el := time.Since(start); el < ceiling {
		t.Fatalf("Stop returned after %v, before the %v ceiling", el, ceiling)
	}
}

func TestServer_StopPauseNotServing(t *testing.T) {
	logger := zerolog.New(io.Discard)
	srv, lis := bufServer(t, logger, grpckit.WithShutdownPause(time.Minute))
	conn := bufDial(t, lis, logger)
	recv, cancel := watchStream(t, conn)

	ctx, ctxCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ctxCancel()
	done := make(chan struct{})
	go func() { _ = srv.Stop(ctx); close(done) }()

	// During the pause health is already NOT_SERVING while Stop has not returned yet.
	if s, err := recv(); err != nil || s != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("during pause: status = %v, err = %v; want NOT_SERVING", s, err)
	}
	select {
	case <-done:
		t.Fatal("Stop returned during the pause")
	default:
	}
	cancel()
	ctxCancel() // interrupts the pause -> forced stop
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop did not abort the pause on ctx cancellation")
	}
}

func TestServer_StopIdempotent(t *testing.T) {
	logger := zerolog.New(io.Discard)
	srv, _ := bufServer(t, logger, grpckit.WithShutdownPause(10*time.Millisecond))
	ctx := ctxTimeout(t, 5*time.Second)
	if err := srv.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := srv.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("repeated Stop is not immediate")
	}
}

func TestServer_StopBeforeStart(t *testing.T) {
	srv, err := grpckit.NewServer(zerolog.New(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := srv.Stop(ctxTimeout(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Stop before Start must not wait for the pause")
	}
}

// --- log levels ------------------------------------------------------------

func lastLevel(t *testing.T, b *syncBuffer, msg string) string {
	t.Helper()
	for _, e := range b.entries(t) {
		if e["message"] == msg {
			return e["level"].(string)
		}
	}
	t.Fatalf("no log entry %q", msg)
	return ""
}

func TestLogging_SuccessLevel(t *testing.T) {
	run := func(t *testing.T, sOpts []grpckit.ServerOption, cOpts []grpckit.ClientOption) (srvLogs, cliLogs *syncBuffer) {
		srvLogs, cliLogs = &syncBuffer{}, &syncBuffer{}
		srv, lis := bufServer(t, zerolog.New(srvLogs), sOpts...)
		(&testService{}).register(srv.Registrar(), nil)
		conn := bufDial(t, lis, zerolog.New(cliLogs), cOpts...)
		ctx := ctxTimeout(t, 5*time.Second)
		if err := invoke(ctx, conn, "Ok"); err != nil {
			t.Fatalf("unary: %v", err)
		}
		if err := runStream(ctx, conn); err != nil {
			t.Fatalf("stream: %v", err)
		}
		return srvLogs, cliLogs
	}

	t.Run("default debug", func(t *testing.T) {
		s, c := run(t, nil, nil)
		for msg, b := range map[string]*syncBuffer{
			"grpc unary handled": s, "grpc stream handled": s, "grpc unary call": c,
		} {
			if got := lastLevel(t, b, msg); got != "debug" {
				t.Errorf("%s: level = %s, want debug", msg, got)
			}
		}
	})
	t.Run("custom info", func(t *testing.T) {
		s, c := run(t,
			[]grpckit.ServerOption{grpckit.WithSuccessLogLevel(zerolog.InfoLevel)},
			[]grpckit.ClientOption{grpckit.WithClientSuccessLogLevel(zerolog.InfoLevel)})
		for msg, b := range map[string]*syncBuffer{
			"grpc unary handled": s, "grpc stream handled": s, "grpc unary call": c,
		} {
			if got := lastLevel(t, b, msg); got != "info" {
				t.Errorf("%s: level = %s, want info", msg, got)
			}
		}
	})
	t.Run("errors unchanged", func(t *testing.T) {
		var logs syncBuffer
		srv, lis := bufServer(t, zerolog.New(&logs), grpckit.WithSuccessLogLevel(zerolog.DebugLevel))
		(&testService{}).register(srv.Registrar(), nil)
		conn := bufDial(t, lis, zerolog.New(io.Discard))
		_ = invoke(ctxTimeout(t, 5*time.Second), conn, "Unlisted")
		if got := lastLevel(t, &logs, "grpc unary handled"); got != "error" {
			t.Errorf("Unavailable: level = %s, want error", got)
		}
	})
}

// --- trace ID limits ------------------------------------------------------------

func TestTrace_IncomingTruncated(t *testing.T) {
	long := strings.Repeat("a", 1000)
	var logs syncBuffer
	got := make(chan string, 1)
	srv, lis := bufServer(t, zerolog.New(&logs))
	(&testService{}).register(srv.Registrar(), func(ctx context.Context) { got <- grpckit.TraceIDFromContext(ctx) })
	conn := bufDial(t, lis, zerolog.New(io.Discard))

	ctx := metadata.AppendToOutgoingContext(ctxTimeout(t, 5*time.Second), grpckit.TraceMetadataKey, long)
	if err := invoke(ctx, conn, "Ok"); err != nil {
		t.Fatal(err)
	}
	if id := <-got; id != long[:128] {
		t.Fatalf("trace_id in ctx: len %d, want 128", len(id))
	}
	for _, e := range logs.entries(t) {
		if e["message"] == "grpc unary handled" {
			if id, _ := e[grpckit.TraceKey].(string); len(id) != 128 {
				t.Fatalf("trace_id in log: len %d, want 128", len(id))
			}
			return
		}
	}
	t.Fatal("no call log entry")
}

func TestTrace_IncomingTruncatedUTF8(t *testing.T) {
	fromMD := func(v string) string {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(grpckit.TraceMetadataKey, v))
		return grpckit.TraceIDFromIncomingMD(ctx)
	}
	// "€" is 3 bytes: 128 is not a multiple of 3, so it is cut to 126 without splitting a rune.
	got := fromMD(strings.Repeat("€", 1000))
	if !utf8Valid(got) || len(got) != 126 {
		t.Fatalf("len = %d valid = %v, want 126 valid", len(got), utf8Valid(got))
	}
	if short := fromMD("short-id"); short != "short-id" {
		t.Fatalf("short trace_id was modified: %q", short)
	}
	if exact := strings.Repeat("é", 64); fromMD(exact) != exact {
		t.Fatal("a trace_id of exactly 128 bytes must not be truncated")
	}
}

func utf8Valid(s string) bool { return strings.ToValidUTF8(s, "") == s }

// --- HealthCheck -------------------------------------------------------------------------------

func TestHealthCheck_ProbeErrorKeepsCode(t *testing.T) {
	logger := zerolog.New(io.Discard)

	// Unreachable server: the dialer always fails.
	conn, err := grpckit.Dial(context.Background(), logger,
		grpckit.WithTarget(bufTarget),
		grpckit.WithDialOptions(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return nil, errors.New("unreachable")
		})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	err = grpckit.HealthCheck(ctxTimeout(t, 3*time.Second), conn, "")
	if !errors.Is(err, grpckit.ErrHealthCheck) || status.Code(err) != codes.Unavailable {
		t.Fatalf("err = %v, code = %v; want ErrHealthCheck + Unavailable", err, status.Code(err))
	}

	// NOT_SERVING → ErrNotServing.
	srv, lis := bufServer(t, logger)
	srv.Health().SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	live := bufDial(t, lis, logger)
	if err := grpckit.HealthCheck(ctxTimeout(t, 3*time.Second), live, ""); !errors.Is(err, grpckit.ErrNotServing) {
		t.Fatalf("err = %v, want ErrNotServing", err)
	}
}
