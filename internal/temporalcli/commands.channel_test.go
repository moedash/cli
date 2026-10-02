package temporalcli_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/cli/internal/temporalcli"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeChannelService answers the channel calls from canned responses and
// records what the CLI sent, so the commands are checked without a server
// that carries channels.
type fakeChannelService struct {
	workflowservice.UnimplementedWorkflowServiceServer

	mu         sync.Mutex
	notifies   []*workflowservice.NotifyChannelRequest
	registers  []*workflowservice.RegisterChannelListenerRequest
	unregister []*workflowservice.UnregisterChannelListenerRequest
	polls      []*workflowservice.PollChannelRequest
	describes  []*workflowservice.DescribeChannelRequest

	err      error
	describe *workflowservice.DescribeChannelResponse
	// pollAnswers are handed out in order; past the end a poll waits for the
	// caller to give up, the way an idle channel does.
	pollAnswers [][]*notificationpb.Notification
	// onPoll runs with the number of polls seen so far.
	onPoll func(n int)
	// describeWorkflow answers the workflow description with its channels.
	describeWorkflow *workflowservice.DescribeWorkflowExecutionResponse
}

func (f *fakeChannelService) DescribeWorkflowExecution(
	context.Context, *workflowservice.DescribeWorkflowExecutionRequest,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.describeWorkflow, nil
}

func (f *fakeChannelService) GetSystemInfo(
	context.Context, *workflowservice.GetSystemInfoRequest,
) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{}, nil
}

func (f *fakeChannelService) NotifyChannel(
	_ context.Context, req *workflowservice.NotifyChannelRequest,
) (*workflowservice.NotifyChannelResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.notifies = append(f.notifies, req)
	if f.err != nil {
		return nil, f.err
	}
	return &workflowservice.NotifyChannelResponse{ListenerCount: 3}, nil
}

func (f *fakeChannelService) RegisterChannelListener(
	_ context.Context, req *workflowservice.RegisterChannelListenerRequest,
) (*workflowservice.RegisterChannelListenerResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers = append(f.registers, req)
	if f.err != nil {
		return nil, f.err
	}
	return &workflowservice.RegisterChannelListenerResponse{ListenerId: "listener-7"}, nil
}

func (f *fakeChannelService) UnregisterChannelListener(
	_ context.Context, req *workflowservice.UnregisterChannelListenerRequest,
) (*workflowservice.UnregisterChannelListenerResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unregister = append(f.unregister, req)
	if f.err != nil {
		return nil, f.err
	}
	return &workflowservice.UnregisterChannelListenerResponse{}, nil
}

func (f *fakeChannelService) DescribeChannel(
	_ context.Context, req *workflowservice.DescribeChannelRequest,
) (*workflowservice.DescribeChannelResponse, error) {
	f.mu.Lock()
	f.describes = append(f.describes, req)
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return f.describe, nil
}

func (f *fakeChannelService) PollChannel(
	ctx context.Context, req *workflowservice.PollChannelRequest,
) (*workflowservice.PollChannelResponse, error) {
	f.mu.Lock()
	f.polls = append(f.polls, req)
	n := len(f.polls)
	var answer []*notificationpb.Notification
	idle := n > len(f.pollAnswers)
	if !idle {
		answer = f.pollAnswers[n-1]
	}
	onPoll, err := f.onPoll, f.err
	f.mu.Unlock()
	if onPoll != nil {
		onPoll(n)
	}
	if err != nil {
		return nil, err
	}
	if idle {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &workflowservice.PollChannelResponse{Notifications: answer}, nil
}

func startFakeChannelService(t *testing.T, f *fakeChannelService) string {
	return startFakeServices(t, f, nil)
}

// startFakeServices serves a fake workflow service and, when given, a fake
// stream service on one address, the way the Service's frontend does.
func startFakeServices(
	t *testing.T, wf workflowservice.WorkflowServiceServer, st streampb.StreamServiceServer,
) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// The Service sends its errors as gRPC statuses; a bare server would send
	// them as unknown errors and lose their kind.
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, serviceerror.ToStatus(err).Err()
		}
		return resp, nil
	}))
	workflowservice.RegisterWorkflowServiceServer(srv, wf)
	if st != nil {
		streampb.RegisterStreamServiceServer(srv, st)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

func jsonPayload(data string) *commonpb.Payload {
	return &commonpb.Payload{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     []byte(data),
	}
}

func TestChannel_ArgumentValidation(t *testing.T) {
	f := &fakeChannelService{}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"notify", "--position", "1", "--counter", "1"}, `"channel" not set`},
		{[]string{"notify", "-c", "ch", "--counter", "1"}, `"position" not set`},
		{[]string{"notify", "-c", "ch", "--position", "1", "--counter", "0"},
			"--counter must be greater than zero"},
		{[]string{"notify", "-c", "ch", "--position", "1", "--counter", "1",
			"--metadata", "topic"}, "must be KEY=VALUE"},
		{[]string{"notify", "-c", "ch", "--position", "1", "--counter", "1",
			"--metadata", "topic=scores"}, "not valid JSON"},
		{[]string{"notify", "-c", "ch", "--position", "1", "--counter", "1",
			"--metadata", `a="x"`, "--metadata", `a="y"`}, "more than once"},
		{[]string{"listener", "add", "-c", "ch"}, `"callback-url" not set`},
		{[]string{"listener", "add", "-c", "ch", "--callback-url", "http://x",
			"--header", "nope"}, "invalid --header"},
		{[]string{"listener", "remove", "-c", "ch"}, `"listener-id" not set`},
		{[]string{"poll", "-c", "ch", "--after-counter", "-1"}, "cannot be negative"},
		{[]string{"poll", "-c", "ch", "--max", "-1"}, "cannot be negative"},
		{[]string{"poll", "-c", "ch", "--wait", "0s"}, "--wait must be positive"},
		{[]string{"describe"}, `"channel" not set`},
		{[]string{"describe", "-c", "ch", "--run-id", "r1"}, "--run-id requires --workflow-id"},
		{[]string{"notify", "-c", "ch", "--position", "1", "--counter", "1", "-r", "r1"},
			"--run-id requires --workflow-id"},
		{[]string{"poll", "-c", "ch", "-r", "r1"}, "--run-id requires --workflow-id"},
		{[]string{"listener", "add", "-c", "ch", "--callback-url", "http://x", "-r", "r1"},
			"--run-id requires --workflow-id"},
		{[]string{"listener", "remove", "-c", "ch", "--listener-id", "l", "-r", "r1"},
			"--run-id requires --workflow-id"},
	} {
		res := h.Execute(append(append([]string{"channel"}, tc.args...), "--address", addr)...)
		require.Error(t, res.Err, tc.args)
		assert.Contains(t, res.Err.Error(), tc.want, tc.args)
	}
	// None of the refused commands reached the Service.
	assert.Empty(t, f.notifies)
	assert.Empty(t, f.registers)
	assert.Empty(t, f.unregister)
	assert.Empty(t, f.polls)
	assert.Empty(t, f.describes)
}

func TestChannel_Notify(t *testing.T) {
	f := &fakeChannelService{}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute(
		"channel", "notify", "--address", addr, "--namespace", "ns1",
		"-c", "stream/scores", "--position", "42", "--counter", "42",
		"--metadata", `topic="scores"`, "--metadata", `batch={"n": 2}`,
	)
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "Notified channel stream/scores", "3")

	require.Len(t, f.notifies, 1)
	req := f.notifies[0]
	assert.Equal(t, "ns1", req.GetNamespace())
	assert.NotEmpty(t, req.GetIdentity())
	assert.NotEmpty(t, req.GetRequestId())
	n := req.GetNotification()
	assert.Equal(t, "stream/scores", n.GetChannel())
	assert.Equal(t, []byte("42"), n.GetPosition())
	assert.Equal(t, int64(42), n.GetCounter())
	require.Len(t, n.GetMetadata(), 2)
	assert.Equal(t, "json/plain", string(n.GetMetadata()["topic"].GetMetadata()["encoding"]))
	assert.Equal(t, `"scores"`, string(n.GetMetadata()["topic"].GetData()))
	assert.Equal(t, `{"n": 2}`, string(n.GetMetadata()["batch"].GetData()))

	// A retried command is a new request, so the Service does not fold it
	// with the first.
	res = h.Execute(
		"channel", "notify", "--address", addr, "-c", "ch", "--position", "p",
		"--counter", "1", "-o", "json",
	)
	require.NoError(t, res.Err)
	var out workflowservice.NotifyChannelResponse
	require.NoError(t, temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &out, true))
	assert.Equal(t, int32(3), out.GetListenerCount())
	require.Len(t, f.notifies, 2)
	assert.NotEqual(t, f.notifies[0].GetRequestId(), f.notifies[1].GetRequestId())
}

func TestChannel_Describe(t *testing.T) {
	registered := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	f := &fakeChannelService{describe: &workflowservice.DescribeChannelResponse{
		Listeners: []*notificationpb.ChannelListener{
			{
				ListenerId: "l-callback",
				Listener: &notificationpb.ChannelListener_Callback{Callback: &commonpb.Callback{
					Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
						Url:    "https://example.com/notify",
						Header: map[string]string{"Authorization": "Bearer secret"},
					}},
				}},
				RegisteredTime: timestamppb.New(registered),
			},
			{
				ListenerId: "l-workflow",
				Listener: &notificationpb.ChannelListener_Workflow{
					Workflow: &notificationpb.WorkflowListener{WorkflowId: "wf-1", RunId: "run-1"},
				},
				RegisteredTime: timestamppb.New(registered),
			},
		},
		Latest: &notificationpb.Notification{
			Channel:  "ch",
			Position: []byte{0x00, 0xff},
			Counter:  9,
			Metadata: map[string]*commonpb.Payload{
				"topic": jsonPayload(`"scores"`),
				"a":     jsonPayload(`1`),
			},
		},
		RetainedCount: 4,
	}}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute("channel", "describe", "--address", addr, "-c", "ch")
	require.NoError(t, res.Err)
	out := res.Stdout.String()
	h.ContainsOnSameLine(out, "Channel", "ch")
	h.ContainsOnSameLine(out, "RetainedCount", "4")
	h.ContainsOnSameLine(out, "LatestCounter", "9")
	// Bytes a terminal would mangle print as base64.
	h.ContainsOnSameLine(out, "LatestPosition", "base64:AP8=")
	h.ContainsOnSameLine(out, "LatestMetadata", `a=1 topic="scores"`)
	h.ContainsOnSameLine(out, "Workflow listeners: 1")
	h.ContainsOnSameLine(out, "l-workflow", "wf-1", "run-1")
	h.ContainsOnSameLine(out, "Callback listeners: 1")
	h.ContainsOnSameLine(out, "l-callback", "https://example.com/notify")
	assert.NotContains(t, out, "secret", "headers stay out of the text output")

	res = h.Execute("channel", "describe", "--address", addr, "-c", "ch", "-o", "json")
	require.NoError(t, res.Err)
	var described workflowservice.DescribeChannelResponse
	require.NoError(t, temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &described, true))
	assert.Equal(t, int32(4), described.GetRetainedCount())
	assert.Len(t, described.GetListeners(), 2)
	assert.Equal(t, []byte{0x00, 0xff}, described.GetLatest().GetPosition())

	// A channel that has seen nothing yet still describes.
	f.describe = &workflowservice.DescribeChannelResponse{}
	res = h.Execute("channel", "describe", "--address", addr, "-c", "ch")
	require.NoError(t, res.Err)
	assert.NotContains(t, res.Stdout.String(), "LatestCounter")
	h.ContainsOnSameLine(res.Stdout.String(), "Workflow listeners: 0")
}

func TestChannel_Listener(t *testing.T) {
	f := &fakeChannelService{}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute(
		"channel", "listener", "add", "--address", addr, "-c", "ch",
		"--callback-url", "https://example.com/notify",
		"--header", "Authorization=Bearer t=1", "--header", "X-Team=ai",
	)
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "Added listener listener-7 to channel ch")
	require.Len(t, f.registers, 1)
	reg := f.registers[0]
	assert.Equal(t, "ch", reg.GetChannel())
	assert.NotEmpty(t, reg.GetRequestId())
	assert.NotEmpty(t, reg.GetIdentity())
	nexus := reg.GetCallback().GetNexus()
	assert.Equal(t, "https://example.com/notify", nexus.GetUrl())
	assert.Equal(t, map[string]string{
		"Authorization": "Bearer t=1",
		"X-Team":        "ai",
	}, nexus.GetHeader())

	res = h.Execute(
		"channel", "listener", "remove", "--address", addr, "-c", "ch",
		"--listener-id", "listener-7",
	)
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "Removed listener listener-7 from channel ch")
	require.Len(t, f.unregister, 1)
	assert.Equal(t, "listener-7", f.unregister[0].GetListenerId())
	assert.Equal(t, "ch", f.unregister[0].GetChannel())
}

func TestChannel_Poll(t *testing.T) {
	f := &fakeChannelService{pollAnswers: [][]*notificationpb.Notification{{
		{Channel: "ch", Position: []byte("offset-4"), Counter: 4,
			Metadata: map[string]*commonpb.Payload{"topic": jsonPayload(`"scores"`)}},
		{Channel: "ch", Position: []byte("offset-6"), Counter: 6},
	}}}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute(
		"channel", "poll", "--address", addr, "-c", "ch",
		"--after-counter", "3", "--wait", "5s", "--max", "10",
	)
	require.NoError(t, res.Err)
	out := res.Stdout.String()
	h.ContainsOnSameLine(out, "Counter", "Position", "Metadata")
	h.ContainsOnSameLine(out, "4", "offset-4", `topic="scores"`)
	h.ContainsOnSameLine(out, "6", "offset-6")
	require.Len(t, f.polls, 1)
	assert.Equal(t, int64(3), f.polls[0].GetAfterCounter())
	assert.Equal(t, 5*time.Second, f.polls[0].GetWait().AsDuration())
	assert.Equal(t, int32(10), f.polls[0].GetMaxNotifications())

	// Nothing retained and nothing arriving within the wait reads as such.
	f.pollAnswers = append(f.pollAnswers, nil)
	res = h.Execute("channel", "poll", "--address", addr, "-c", "ch", "--after-counter", "6")
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "No notifications on channel ch after counter 6")
}

func TestChannel_PollFollow(t *testing.T) {
	h := NewCommandHarness(t)
	f := &fakeChannelService{
		pollAnswers: [][]*notificationpb.Notification{
			{{Channel: "ch", Position: []byte("a"), Counter: 2}},
			{},
			{
				{Channel: "ch", Position: []byte("b"), Counter: 5},
				{Channel: "ch", Position: []byte("c"), Counter: 3},
			},
		},
		// The fourth poll is the command waiting on an idle channel; stop it
		// there the way an interrupt would.
		onPoll: func(n int) {
			if n == 4 {
				h.CancelContext()
			}
		},
	}
	addr := startFakeChannelService(t, f)

	res := h.Execute("channel", "poll", "--address", addr, "-c", "ch", "--follow", "-o", "jsonl")
	require.NoError(t, res.Err)
	var counters []int64
	for _, raw := range decodeJSONValues(t, res.Stdout.String()) {
		var n notificationpb.Notification
		require.NoError(t, temporalcli.UnmarshalProtoJSONWithOptions(raw, &n, true))
		counters = append(counters, n.GetCounter())
	}
	assert.Equal(t, []int64{2, 5, 3}, counters)

	// Each poll starts after the highest counter seen so far, and an empty
	// answer keeps the cursor where it was.
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.polls, 4)
	var after []int64
	for _, p := range f.polls {
		after = append(after, p.GetAfterCounter())
	}
	assert.Equal(t, []int64{0, 2, 2, 5}, after)
}

func TestChannel_PlainRefusals(t *testing.T) {
	f := &fakeChannelService{}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	f.err = serviceerror.NewNotFound("channel not found")
	res := h.Execute("channel", "describe", "--address", addr, "-c", "missing")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), `there is no channel "missing"`)
	assert.Contains(t, res.Err.Error(), "The server said: channel not found")
	res = h.Execute("channel", "poll", "--address", addr, "-c", "missing")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), `there is no channel "missing"`)

	f.err = serviceerror.NewResourceExhaustedf(enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
		"channel already has 1000 listeners, which is the limit")
	res = h.Execute(
		"channel", "listener", "add", "--address", addr, "-c", "ch", "--callback-url", "http://x",
	)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "as many listeners as it may hold")
	assert.Contains(t, res.Err.Error(), "1000 listeners")

	f.err = &serviceerror.ResourceExhausted{
		Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
		Message: "namespace is over its limit",
	}
	res = h.Execute(
		"channel", "notify", "--address", addr, "-c", "ch", "--position", "1", "--counter", "1",
	)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "notifying channels faster than it may")

	// Errors the CLI has nothing to add to pass through as they came.
	f.err = serviceerror.NewInvalidArgument("notification position exceeds 1024 bytes")
	res = h.Execute(
		"channel", "notify", "--address", addr, "-c", "ch", "--position", "1", "--counter", "1",
	)
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "position exceeds 1024 bytes")
	assert.NotContains(t, res.Err.Error(), "The server said")
}

// decodeJSONValues splits output holding one JSON value after another.
func decodeJSONValues(t *testing.T, s string) []json.RawMessage {
	var out []json.RawMessage
	dec := json.NewDecoder(strings.NewReader(s))
	for dec.More() {
		var raw json.RawMessage
		require.NoError(t, dec.Decode(&raw))
		out = append(out, raw)
	}
	return out
}

func (s *SharedServerSuite) TestChannel_NotifyWithoutListenersIsRetained() {
	ch := "channel-" + uuid.NewString()
	res := s.Execute(
		"channel", "notify", "--address", s.Address(), "-c", ch,
		"--position", "offset-1", "--counter", "1", "--metadata", `topic="scores"`,
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Listeners reached", "0")

	res = s.Execute("channel", "describe", "--address", s.Address(), "-c", ch)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "RetainedCount", "1")
	s.ContainsOnSameLine(out, "LatestCounter", "1")
	s.ContainsOnSameLine(out, "LatestPosition", "offset-1")
	s.ContainsOnSameLine(out, "LatestMetadata", `topic="scores"`)
	s.ContainsOnSameLine(out, "Callback listeners: 0")
}

func (s *SharedServerSuite) TestChannel_StaleCounterIsFolded() {
	ch := "channel-" + uuid.NewString()
	notify := func(position, counter string) {
		res := s.Execute(
			"channel", "notify", "--address", s.Address(), "-c", ch,
			"--position", position, "--counter", counter,
		)
		s.NoError(res.Err)
	}
	notify("p5", "5")
	// An equal or lower counter is folded into the one already kept.
	notify("p5-again", "5")
	notify("p3", "3")
	res := s.Execute("channel", "describe", "--address", s.Address(), "-c", ch)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "RetainedCount", "1")
	s.ContainsOnSameLine(res.Stdout.String(), "LatestPosition", "p5")

	notify("p7", "7")
	notify("p9", "9")
	res = s.Execute(
		"channel", "poll", "--address", s.Address(), "-c", ch,
		"--after-counter", "5", "--wait", "2s", "-o", "jsonl",
	)
	s.NoError(res.Err)
	var counters []int64
	for _, raw := range decodeJSONValues(s.T(), res.Stdout.String()) {
		var n notificationpb.Notification
		s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(raw, &n, true))
		counters = append(counters, n.GetCounter())
	}
	s.Equal([]int64{7, 9}, counters)
}

func (s *SharedServerSuite) TestChannel_PollWaitsForTheNextNotification() {
	ch := "channel-" + uuid.NewString()
	res := s.Execute(
		"channel", "notify", "--address", s.Address(), "-c", ch, "--position", "p1", "--counter", "1",
	)
	s.NoError(res.Err)

	done := make(chan *CommandResult, 1)
	started := time.Now()
	go func() {
		done <- s.Execute(
			"channel", "poll", "--address", s.Address(), "-c", ch,
			"--after-counter", "1", "--wait", "20s", "-o", "json",
		)
	}()
	// The poll is parked on the channel by the time this lands.
	time.Sleep(time.Second)
	res = s.Execute(
		"channel", "notify", "--address", s.Address(), "-c", ch, "--position", "p2", "--counter", "2",
	)
	s.NoError(res.Err)

	select {
	case res = <-done:
	case <-time.After(60 * time.Second):
		s.Fail("poll did not return after the notification")
	}
	s.NoError(res.Err)
	s.Less(time.Since(started), 15*time.Second, "the poll answered on arrival, not at its wait")
	raw := decodeJSONValues(s.T(), res.Stdout.String())
	s.Len(raw, 1)
	var list []json.RawMessage
	s.NoError(json.Unmarshal(raw[0], &list))
	s.Len(list, 1)
	var n notificationpb.Notification
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(list[0], &n, true))
	s.Equal(int64(2), n.GetCounter())
	s.Equal([]byte("p2"), n.GetPosition())
}

func (s *SharedServerSuite) TestChannel_CallbackListenerRoundTrip() {
	type delivery struct {
		auth string
		body []byte
	}
	deliveries := make(chan delivery, 8)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		deliveries <- delivery{auth: r.Header.Get("Authorization"), body: body}
	}))
	defer receiver.Close()

	ch := "channel-" + uuid.NewString()
	res := s.Execute(
		"channel", "listener", "add", "--address", s.Address(), "-c", ch,
		"--callback-url", receiver.URL+"/notify", "--header", "Authorization=Bearer t1",
		"-o", "json",
	)
	s.NoError(res.Err)
	var added workflowservice.RegisterChannelListenerResponse
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &added, true))
	s.NotEmpty(added.GetListenerId())

	res = s.Execute("channel", "describe", "--address", s.Address(), "-c", ch)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Callback listeners: 1")
	s.ContainsOnSameLine(res.Stdout.String(), added.GetListenerId(), receiver.URL+"/notify")

	res = s.Execute(
		"channel", "notify", "--address", s.Address(), "-c", ch, "--position", "p1", "--counter", "1",
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Listeners reached", "1")
	select {
	case d := <-deliveries:
		s.Equal("Bearer t1", d.auth)
		s.Contains(string(d.body), ch)
	case <-time.After(30 * time.Second):
		s.Fail("the callback was not called")
	}

	res = s.Execute(
		"channel", "listener", "remove", "--address", s.Address(), "-c", ch,
		"--listener-id", added.GetListenerId(),
	)
	s.NoError(res.Err)
	res = s.Execute("channel", "describe", "--address", s.Address(), "-c", ch)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Callback listeners: 0")
}

func (s *SharedServerSuite) TestChannel_UnknownChannel() {
	res := s.Execute("channel", "describe", "--address", s.Address(), "-c", "never-"+uuid.NewString())
	s.ErrorContains(res.Err, "there is no channel")
}

func TestChannel_LinkedToWorkflow(t *testing.T) {
	f := &fakeChannelService{
		describe: &workflowservice.DescribeChannelResponse{
			Kind:          notificationpb.CHANNEL_KIND_LINKED,
			LinkedTo:      &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-9"},
			RetainedCount: 1,
			Latest:        &notificationpb.Notification{Channel: "ch", Position: []byte("p1"), Counter: 1},
			Listeners: []*notificationpb.ChannelListener{{
				ListenerId: "wf-1",
				Listener: &notificationpb.ChannelListener_Workflow{
					Workflow: &notificationpb.WorkflowListener{WorkflowId: "wf-1", RunId: "run-9"},
				},
			}},
		},
		pollAnswers: [][]*notificationpb.Notification{{{Channel: "ch", Counter: 1}}},
	}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)
	linked := []string{"--address", addr, "-c", "ch", "--workflow-id", "wf-1"}

	res := h.Execute(append([]string{"channel", "notify", "--position", "p1", "--counter", "1",
		"--run-id", "run-9"}, linked...)...)
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "Notified channel ch of workflow wf-1")
	res = h.Execute(append([]string{"channel", "describe"}, linked...)...)
	require.NoError(t, res.Err)
	out := res.Stdout.String()
	h.ContainsOnSameLine(out, "Kind", "Linked")
	h.ContainsOnSameLine(out, "WorkflowId", "wf-1")
	h.ContainsOnSameLine(out, "RunId", "run-9")
	h.ContainsOnSameLine(out, "RetainedCount", "1")
	// The owner listens without registering, so its row has no time.
	h.ContainsOnSameLine(out, "Workflow listeners: 1")
	assert.NotContains(t, out, "ago")
	res = h.Execute(append([]string{"channel", "poll"}, linked...)...)
	require.NoError(t, res.Err)
	res = h.Execute(append([]string{"channel", "listener", "add", "--callback-url",
		"http://x"}, linked...)...)
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "to channel ch of workflow wf-1")
	res = h.Execute(append([]string{"channel", "listener", "remove", "--listener-id",
		"listener-7"}, linked...)...)
	require.NoError(t, res.Err)

	// Every call names the workflow; the run only where it was given.
	f.mu.Lock()
	defer f.mu.Unlock()
	require.Len(t, f.notifies, 1)
	assert.Equal(t, "wf-1", f.notifies[0].GetWorkflowExecution().GetWorkflowId())
	assert.Equal(t, "run-9", f.notifies[0].GetWorkflowExecution().GetRunId())
	require.Len(t, f.describes, 1)
	assert.Equal(t, "wf-1", f.describes[0].GetWorkflowExecution().GetWorkflowId())
	assert.Empty(t, f.describes[0].GetWorkflowExecution().GetRunId())
	require.Len(t, f.polls, 1)
	assert.Equal(t, "wf-1", f.polls[0].GetWorkflowExecution().GetWorkflowId())
	require.Len(t, f.registers, 1)
	assert.Equal(t, "wf-1", f.registers[0].GetWorkflowExecution().GetWorkflowId())
	require.Len(t, f.unregister, 1)
	assert.Equal(t, "wf-1", f.unregister[0].GetWorkflowExecution().GetWorkflowId())
}

func TestChannel_IndependentByDefault(t *testing.T) {
	f := &fakeChannelService{describe: &workflowservice.DescribeChannelResponse{}}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute("channel", "notify", "--address", addr, "-c", "ch",
		"--position", "p", "--counter", "1")
	require.NoError(t, res.Err)
	// A server that predates the linked kind leaves the kind unset.
	res = h.Execute("channel", "describe", "--address", addr, "-c", "ch")
	require.NoError(t, res.Err)
	h.ContainsOnSameLine(res.Stdout.String(), "Kind", "Independent")
	assert.NotContains(t, res.Stdout.String(), "WorkflowId")
	assert.Nil(t, f.notifies[0].GetWorkflowExecution())
	assert.Nil(t, f.describes[0].GetWorkflowExecution())
}

func TestChannel_LinkedNotFound(t *testing.T) {
	f := &fakeChannelService{err: serviceerror.NewNotFound("workflow execution already completed")}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute("channel", "notify", "--address", addr, "-c", "ch", "-w", "gone",
		"--position", "p", "--counter", "1")
	require.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), `workflow "gone" has no running execution`)
	assert.Contains(t, res.Err.Error(), `channel "ch" of workflow "gone"`)
}

// runningWorkflowDescription is a description with what the text output
// dereferences, for a run that is still open so no close event is fetched.
func runningWorkflowDescription(
	subs ...*workflowpb.ChannelSubscriptionInfo,
) *workflowservice.DescribeWorkflowExecutionResponse {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: "run-1"},
			Type:      &commonpb.WorkflowType{Name: "DevWorkflow"},
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)),
		},
		ChannelSubscriptions: subs,
	}
}

func TestChannel_WorkflowDescribeListsChannels(t *testing.T) {
	f := &fakeChannelService{describeWorkflow: runningWorkflowDescription(
		&workflowpb.ChannelSubscriptionInfo{
			Channel:             "demo",
			Kind:                notificationpb.CHANNEL_KIND_INDEPENDENT,
			SubscribedEventId:   5,
			LastCounter:         4,
			PendingNotification: &notificationpb.Notification{Channel: "demo", Counter: 5},
		},
		&workflowpb.ChannelSubscriptionInfo{
			Channel:          "stream/scores",
			Kind:             notificationpb.CHANNEL_KIND_LINKED,
			LastCounter:      3,
			ScheduledCounter: 0,
			ListenerCount:    2,
			RetainedCount:    16,
			AcceptedCount:    3,
		},
	)}
	addr := startFakeChannelService(t, f)
	h := NewCommandHarness(t)

	res := h.Execute("workflow", "describe", "--address", addr, "-w", "wf-1")
	require.NoError(t, res.Err)
	out := res.Stdout.String()
	h.ContainsOnSameLine(out, "Notification Channels: 2")
	h.ContainsOnSameLine(out, "Channel", "Kind", "LastCounter", "PendingCounter",
		"ScheduledCounter", "Listeners", "Retained")
	// The pending column is blank, not zero, when nothing is pending.
	assert.Equal(t, []string{"demo", "Independent", "4", "5", "0", "0", "0"},
		strings.Fields(lineContaining(t, out, "demo")))
	assert.Equal(t, []string{"stream/scores", "Linked", "3", "0", "2", "16"},
		strings.Fields(lineContaining(t, out, "stream/scores")))

	res = h.Execute("workflow", "describe", "--address", addr, "-w", "wf-1", "-o", "json")
	require.NoError(t, res.Err)
	var described workflowservice.DescribeWorkflowExecutionResponse
	require.NoError(t, temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &described, true))
	require.Len(t, described.GetChannelSubscriptions(), 2)
	assert.Equal(t, int64(5), described.GetChannelSubscriptions()[0].GetSubscribedEventId())
	assert.Equal(t, int64(5),
		described.GetChannelSubscriptions()[0].GetPendingNotification().GetCounter())
	linked := described.GetChannelSubscriptions()[1]
	assert.Equal(t, notificationpb.CHANNEL_KIND_LINKED, linked.GetKind())
	assert.Equal(t, int64(3), linked.GetAcceptedCount())

	// A workflow standing on no channel has no section at all.
	f.describeWorkflow = runningWorkflowDescription()
	res = h.Execute("workflow", "describe", "--address", addr, "-w", "wf-1")
	require.NoError(t, res.Err)
	assert.NotContains(t, res.Stdout.String(), "Notification Channels")
}

func lineContaining(t *testing.T, text, piece string) string {
	for _, line := range strings.Split(text, "\n") {
		if strings.Contains(line, piece) {
			return line
		}
	}
	require.Failf(t, "line not found", "no line contains %q", piece)
	return ""
}

func (s *SharedServerSuite) TestChannel_LinkedToWorkflow() {
	s.Worker().OnDevWorkflow(func(ctx workflow.Context, a any) (any, error) {
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return nil, nil
	})
	run, err := s.Client.ExecuteWorkflow(
		s.Context,
		client.StartWorkflowOptions{TaskQueue: s.Worker().Options.TaskQueue},
		DevWorkflow,
		"ignored",
	)
	s.NoError(err)
	ch := "channel-" + uuid.NewString()
	linked := []string{"--address", s.Address(), "-c", ch, "--workflow-id", run.GetID()}
	channel := func(args ...string) *CommandResult {
		return s.Execute(append(append([]string{"channel"}, args...), linked...)...)
	}

	// The owner listens by construction, so the first notify reaches it.
	res := channel("notify", "--position", "p1", "--counter", "1")
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Listeners reached", "1")

	res = channel("describe")
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "Kind", "Linked")
	s.ContainsOnSameLine(out, "WorkflowId", run.GetID())
	s.ContainsOnSameLine(out, "RunId", run.GetRunID())
	s.ContainsOnSameLine(out, "RetainedCount", "1")
	s.ContainsOnSameLine(out, "LatestPosition", "p1")
	s.ContainsOnSameLine(out, "Workflow listeners: 1")

	res = channel("poll", "--after-counter", "0", "--wait", "5s", "-o", "jsonl")
	s.NoError(res.Err)
	raw := decodeJSONValues(s.T(), res.Stdout.String())
	s.Len(raw, 1)
	var n notificationpb.Notification
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(raw[0], &n, true))
	s.Equal(int64(1), n.GetCounter())
	s.Equal(run.GetID(), n.GetLinkedTo().GetWorkflowId())

	res = channel("listener", "add", "--callback-url", "http://127.0.0.1:1/notify", "-o", "json")
	s.NoError(res.Err)
	var added workflowservice.RegisterChannelListenerResponse
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &added, true))
	res = channel("describe")
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Callback listeners: 1")
	s.ContainsOnSameLine(res.Stdout.String(), added.GetListenerId(), "http://127.0.0.1:1/notify")
	res = channel("listener", "remove", "--listener-id", added.GetListenerId())
	s.NoError(res.Err)
	res = channel("describe")
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Callback listeners: 0")

	// A name nobody has used still exists on a running workflow.
	res = s.Execute("channel", "describe", "--address", s.Address(),
		"-c", "untouched-"+uuid.NewString(), "--workflow-id", run.GetID())
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Kind", "Linked")
	s.ContainsOnSameLine(res.Stdout.String(), "RetainedCount", "0")
	s.ContainsOnSameLine(res.Stdout.String(), "Workflow listeners: 0")
	s.ContainsOnSameLine(res.Stdout.String(), "Callback listeners: 0")

	// The independent channel of the same name is a different channel.
	res = s.Execute("channel", "describe", "--address", s.Address(), "-c", ch)
	s.ErrorContains(res.Err, "there is no channel")

	res = s.Execute("channel", "describe", "--address", s.Address(), "-c", ch, "--run-id", "r")
	s.ErrorContains(res.Err, "--run-id requires --workflow-id")

	s.NoError(s.Client.SignalWorkflow(s.Context, run.GetID(), "", "finish", nil))
	s.NoError(run.Get(s.Context, nil))
}
