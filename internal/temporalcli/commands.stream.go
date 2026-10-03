package temporalcli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/mattn/go-isatty"
	"github.com/temporalio/cli/cliext"
	"github.com/temporalio/cli/internal/printer"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	streamapi "go.temporal.io/api/stream/v1"
	"go.temporal.io/api/temporalproto"
	"go.temporal.io/sdk/converter"
	streamlib "go.temporal.io/server/chasm/lib/stream"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

// streamRefusal is a server refusal whose reason token has been read, so the
// message says what to do instead of carrying the token.
type streamRefusal struct {
	plain  string
	detail string
	cause  error
}

func (e *streamRefusal) Error() string {
	if e.detail == "" {
		return e.plain
	}
	return e.plain + " The server said: " + e.detail
}

func (e *streamRefusal) Unwrap() error { return e.cause }

// plainRefusal maps a refusal's reason token to plain language. Every other
// error comes back as it was.
func plainRefusal(err error) error {
	var refused *serviceerror.FailedPrecondition
	if !errors.As(err, &refused) {
		return err
	}
	reason := streamlib.ReasonOf(refused.Message)
	if reason == "" {
		return err
	}
	_, detail, _ := strings.Cut(refused.Message, ": ")
	var plain string
	switch reason {
	case streamlib.ReasonProducerConflict:
		plain = "the producer already appended different content at this sequence, so " +
			"nothing was written. Use the next sequence, or check that two producers do not " +
			"share the id."
	case streamlib.ReasonProducerStaleSequence:
		plain = "the sequence is below the producer's latest, so nothing was written. A " +
			"producer carries one append at a time; retry with its latest sequence or move on " +
			"to the next."
	case streamlib.ReasonCursorBelowFloor:
		plain = "the read starts below the stream's floor, and those records are gone. Start " +
			"from the beginning, or at the offset the stream now starts at."
	case streamlib.ReasonStreamClosed:
		plain = "the stream is closed. Its records stay readable, but nothing more can be " +
			"appended."
	case streamlib.ReasonPolicyMismatch:
		plain = "a stream with this id already exists with a different lifecycle. Use a new " +
			"id, or repeat the existing retention, max items and max bytes."
	}
	return &streamRefusal{plain: plain, detail: detail, cause: err}
}

// streamTarget is the stream a command names: a standalone stream by its id,
// or a stream an execution owns by that owner and a name.
type streamTarget struct {
	streamID string
	owner    *streampb.StreamOwner
	name     string
}

// resolvedName is the name the server resolves an owned stream to when the
// command leaves it out.
func (t streamTarget) resolvedName() string {
	if t.name == "" {
		return streamlib.DefaultStreamName
	}
	return t.name
}

func (t streamTarget) String() string {
	if t.streamID != "" {
		return fmt.Sprintf("stream %q", t.streamID)
	}
	name := t.resolvedName()
	switch t.owner.GetKind() {
	case streampb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		return fmt.Sprintf("stream %q of activity %q in workflow %q",
			name, t.owner.GetActivityId(), t.owner.GetId())
	case streampb.STREAM_OWNER_KIND_ACTIVITY:
		return fmt.Sprintf("stream %q of activity %q", name, t.owner.GetId())
	default:
		return fmt.Sprintf("stream %q of workflow %q", name, t.owner.GetId())
	}
}

// streamChannelPrefix starts the name of every notification channel a stream
// notifies.
const streamChannelPrefix = "stream/"

// streamChannel is the notification channel a stream notifies on each append
// and on its close. The server derives the name from the stream's identity
// alone, so the CLI derives the same name without a call and a listener can
// follow a stream before it exists.
type streamChannel struct {
	name  string
	kind  notificationpb.ChannelKind
	owner *commonpb.Execution
}

func (t streamTarget) channel() streamChannel {
	if t.streamID != "" {
		return streamChannel{
			name: streamChannelPrefix + t.streamID,
			kind: notificationpb.CHANNEL_KIND_INDEPENDENT,
		}
	}
	owner := &commonpb.Execution{
		Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
		BusinessId: t.owner.GetId(),
		RunId:      t.owner.GetRunId(),
	}
	switch t.owner.GetKind() {
	case streampb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY:
		// The activity's stream notifies the workflow that scheduled it, so the
		// activity ID is in the name and the workflow is the owner.
		return streamChannel{
			name:  streamChannelPrefix + t.owner.GetActivityId() + "/" + t.resolvedName(),
			kind:  notificationpb.CHANNEL_KIND_LINKED,
			owner: owner,
		}
	case streampb.STREAM_OWNER_KIND_ACTIVITY:
		// A standalone activity is an execution that holds linked channels of
		// its own, since the server resolves the channel's owner from the
		// execution a request names. Its stream's channel then needs no
		// activity ID in the name.
		owner.Type = enumspb.EXECUTION_TYPE_ACTIVITY
		return streamChannel{
			name:  streamChannelPrefix + t.resolvedName(),
			kind:  notificationpb.CHANNEL_KIND_LINKED,
			owner: owner,
		}
	default:
		return streamChannel{
			name:  streamChannelPrefix + t.resolvedName(),
			kind:  notificationpb.CHANNEL_KIND_LINKED,
			owner: owner,
		}
	}
}

func (o *StreamReferenceOptions) target() (streamTarget, error) {
	hasOwner := o.WorkflowId != "" || o.ActivityId != ""
	switch {
	case o.StreamId == "" && !hasOwner:
		return streamTarget{}, fmt.Errorf(
			"must set either --stream-id or an owner with --workflow-id or --activity-id")
	case o.StreamId != "" && hasOwner:
		return streamTarget{}, fmt.Errorf("cannot set --stream-id with --workflow-id or --activity-id")
	case o.StreamId != "" && (o.RunId != "" || o.Name != ""):
		return streamTarget{}, fmt.Errorf("--run-id and --name name an owner's stream, not --stream-id")
	case o.StreamId != "":
		return streamTarget{streamID: o.StreamId}, nil
	}
	owner := &streampb.StreamOwner{RunId: o.RunId}
	switch {
	case o.WorkflowId != "" && o.ActivityId != "":
		owner.Kind = streampb.STREAM_OWNER_KIND_WORKFLOW_ACTIVITY
		owner.Id, owner.ActivityId = o.WorkflowId, o.ActivityId
	case o.WorkflowId != "":
		owner.Kind = streampb.STREAM_OWNER_KIND_WORKFLOW
		owner.Id = o.WorkflowId
	default:
		owner.Kind = streampb.STREAM_OWNER_KIND_ACTIVITY
		owner.Id = o.ActivityId
	}
	return streamTarget{owner: owner, name: o.Name}, nil
}

// streamClient reaches the stream service over the connection the SDK dialed,
// so TLS, credentials and headers are the ones every other command uses.
type streamClient struct {
	*dialedClient
	svc       streampb.StreamServiceClient
	namespace string
}

func dialStreamClient(cctx *CommandContext, c *cliext.ClientOptions) (*streamClient, error) {
	cl, err := dialClientWithConn(cctx, c)
	if err != nil {
		return nil, err
	}
	return &streamClient{
		dialedClient: cl,
		svc:          streampb.NewStreamServiceClient(cl.Conn),
		namespace:    c.Namespace,
	}, nil
}

// applyCodec runs every payload the records carry through one codec call, so a
// page costs one round trip to the codec server rather than one per record.
func (s *streamClient) applyCodec(records []*streampb.StreamRecord, encode bool) error {
	if s.PayloadCodec == nil {
		return nil
	}
	fn := s.PayloadCodec.Decode
	if encode {
		fn = s.PayloadCodec.Encode
	}
	var payloads []*commonpb.Payload
	var put []func(*commonpb.Payload)
	for _, r := range records {
		if r.Body != nil {
			payloads = append(payloads, r.Body)
			put = append(put, func(p *commonpb.Payload) { r.Body = p })
		}
		for k, v := range r.Metadata {
			if v == nil {
				continue
			}
			payloads = append(payloads, v)
			put = append(put, func(p *commonpb.Payload) { r.Metadata[k] = p })
		}
	}
	if len(payloads) == 0 {
		return nil
	}
	out, err := fn(payloads)
	if err != nil {
		return err
	}
	if len(out) != len(payloads) {
		return fmt.Errorf("codec returned %d payloads for %d", len(out), len(payloads))
	}
	for i, p := range out {
		put[i](p)
	}
	return nil
}

func (s *streamClient) decodeRecords(records []*streampb.StreamRecord) error {
	return s.applyCodec(records, false)
}

func (s *streamClient) encodeRecords(records []*streampb.StreamRecord) error {
	return s.applyCodec(records, true)
}

func (s *streamClient) describe(
	ctx context.Context, t streamTarget,
) (*streampb.StreamState, error) {
	if t.streamID != "" {
		resp, err := s.svc.DescribeStream(ctx, &streampb.DescribeStreamRequest{
			FrontendRequest: &streampb.DescribeStreamInput{
				Namespace: s.namespace,
				StreamId:  t.streamID,
			},
		})
		return resp.GetFrontendResponse().GetState(), err
	}
	resp, err := s.svc.DescribeWorkflowStream(ctx, &streampb.DescribeWorkflowStreamRequest{
		FrontendRequest: &streampb.DescribeWorkflowStreamInput{
			Namespace:  s.namespace,
			Owner:      t.owner,
			StreamName: t.name,
		},
	})
	return resp.GetFrontendResponse().GetState(), err
}

func (s *streamClient) poll(
	ctx context.Context,
	t streamTarget,
	from int64,
	start *streamapi.StreamStartPosition,
	pageSize int32,
	topics []string,
	wait bool,
) (*streampb.PollMessagesOutput, error) {
	if t.streamID != "" {
		resp, err := s.svc.PollMessages(ctx, &streampb.PollMessagesRequest{
			FrontendRequest: &streampb.PollMessagesInput{
				Namespace:       s.namespace,
				StreamId:        t.streamID,
				FromOffset:      from,
				StartPosition:   start,
				MaxMessages:     pageSize,
				Topics:          topics,
				WaitNewMessages: wait,
			},
		})
		return resp.GetFrontendResponse(), err
	}
	resp, err := s.svc.PollWorkflowMessages(ctx, &streampb.PollWorkflowMessagesRequest{
		FrontendRequest: &streampb.PollWorkflowMessagesInput{
			Namespace:       s.namespace,
			Owner:           t.owner,
			StreamName:      t.name,
			FromOffset:      from,
			StartPosition:   start,
			MaxMessages:     pageSize,
			Topics:          topics,
			WaitNewMessages: wait,
		},
	})
	return resp.GetFrontendResponse(), err
}

func (s *streamClient) append(
	ctx context.Context,
	t streamTarget,
	records []*streampb.StreamRecord,
	producerID string,
	sequence int64,
	expectedOffset *int64,
) (*streampb.AddMessagesOutput, error) {
	if t.streamID != "" {
		in := &streampb.AddMessagesInput{
			Namespace:  s.namespace,
			StreamId:   t.streamID,
			Records:    records,
			ProducerId: producerID,
			Sequence:   sequence,
		}
		if expectedOffset != nil {
			in.ExpectedOffset, in.UseExpectedOffset = *expectedOffset, true
		}
		resp, err := s.svc.AddMessages(ctx, &streampb.AddMessagesRequest{FrontendRequest: in})
		return resp.GetFrontendResponse(), err
	}
	resp, err := s.svc.AddWorkflowMessages(ctx, &streampb.AddWorkflowMessagesRequest{
		FrontendRequest: &streampb.AddWorkflowMessagesInput{
			Namespace:  s.namespace,
			Owner:      t.owner,
			StreamName: t.name,
			Records:    records,
			ProducerId: producerID,
			Sequence:   sequence,
		},
	})
	return resp.GetFrontendResponse(), err
}

func (c *TemporalStreamCreateCommand) run(cctx *CommandContext, _ []string) error {
	if c.MaxItems < 0 || c.MaxBytes < 0 {
		return fmt.Errorf("--max-items and --max-bytes cannot be negative")
	}
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	lifecycle := &streampb.StreamLifecycle{
		MaxItems: int64(c.MaxItems),
		MaxBytes: int64(c.MaxBytes),
	}
	if c.Retention.Duration() > 0 {
		lifecycle.Retention = durationpb.New(c.Retention.Duration())
	}
	resp, err := cl.svc.CreateStream(cctx, &streampb.CreateStreamRequest{
		FrontendRequest: &streampb.CreateStreamInput{
			Namespace: cl.namespace,
			StreamId:  c.StreamId,
			Lifecycle: lifecycle,
		},
	})
	if err != nil {
		return fmt.Errorf("failed creating stream %q: %w", c.StreamId, plainRefusal(err))
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(resp.GetFrontendResponse(), printer.StructuredOptions{})
	}
	cctx.Printer.Printlnf("Created stream %v with run id %v",
		c.StreamId, resp.GetFrontendResponse().GetRunId())
	return nil
}

func (c *TemporalStreamListCommand) run(cctx *CommandContext, _ []string) error {
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	// This is a listing command subject to json vs jsonl rules
	cctx.Printer.StartList()
	defer cctx.Printer.EndList()

	pageSize := c.PageSize
	if c.Limit > 0 && c.Limit < pageSize {
		pageSize = c.Limit
	}
	type row struct {
		StreamId string
		RunId    string
	}
	var token []byte
	var seen int
	for pageIndex := 0; ; pageIndex++ {
		resp, err := cl.svc.ListStreams(cctx, &streampb.ListStreamsRequest{
			FrontendRequest: &streampb.ListStreamsInput{
				Namespace:     cl.namespace,
				PageSize:      int32(pageSize),
				NextPageToken: token,
				Query:         c.Query,
			},
		})
		if err != nil {
			return fmt.Errorf("failed listing streams: %w", err)
		}
		page := resp.GetFrontendResponse()
		var rows []row
		for _, entry := range page.GetStreams() {
			if c.Limit > 0 && seen >= c.Limit {
				break
			}
			seen++
			if cctx.JSONOutput {
				_ = cctx.Printer.PrintStructured(entry, printer.StructuredOptions{})
			} else {
				rows = append(rows, row{StreamId: entry.GetStreamId(), RunId: entry.GetRunId()})
			}
		}
		// Print table, headers only on first table
		if len(rows) > 0 {
			_ = cctx.Printer.PrintStructured(rows, printer.StructuredOptions{
				Table: &printer.TableOptions{NoHeader: pageIndex > 0},
			})
		}
		token = page.GetNextPageToken()
		if len(token) == 0 || (c.Limit > 0 && seen >= c.Limit) {
			return nil
		}
	}
}

func (c *TemporalStreamDescribeCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	state, err := cl.describe(cctx, target)
	if err != nil {
		return fmt.Errorf("failed describing %v: %w", target, err)
	}
	if reason := state.GetCloseReason(); reason != nil && cl.PayloadCodec != nil {
		decoded, err := cl.PayloadCodec.Decode([]*commonpb.Payload{reason})
		if err != nil {
			return fmt.Errorf("failed decoding close reason: %w", err)
		}
		if len(decoded) == 1 {
			state.CloseReason = decoded[0]
		}
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(state, printer.StructuredOptions{})
	}

	// The frontier, the floor and the close state are facts at zero too, so
	// they stay on the card; the rest is shown when set.
	info := struct {
		StreamId       string `cli:",cardOmitEmpty"`
		WorkflowId     string `cli:",cardOmitEmpty"`
		ActivityId     string `cli:",cardOmitEmpty"`
		RunId          string `cli:",cardOmitEmpty"`
		Name           string `cli:",cardOmitEmpty"`
		HeadOffset     int64
		BaseOffset     int64
		Records        int64
		HeldBytes      int64
		AppendedBytes  int64
		Closed         bool
		CloseTime      time.Time         `cli:",cardOmitEmpty"`
		CloseReason    *commonpb.Payload `cli:",cardOmitEmpty"`
		RedirectRunId  string            `cli:",cardOmitEmpty"`
		Retention      time.Duration     `cli:",cardOmitEmpty"`
		MaxItems       int64             `cli:",cardOmitEmpty"`
		MaxBytes       int64             `cli:",cardOmitEmpty"`
		BudgetMaxItems int64             `cli:",cardOmitEmpty"`
		BudgetMaxBytes int64             `cli:",cardOmitEmpty"`
	}{
		StreamId:       target.streamID,
		HeadOffset:     state.GetHeadOffset(),
		BaseOffset:     state.GetBaseOffset(),
		Records:        state.GetHeadOffset() - state.GetBaseOffset(),
		HeldBytes:      state.GetHeldBytes(),
		AppendedBytes:  state.GetAppendedBytes(),
		Closed:         state.GetClosed(),
		CloseReason:    state.GetCloseReason(),
		RedirectRunId:  state.GetRedirectRunId(),
		Retention:      state.GetLifecycle().GetRetention().AsDuration(),
		MaxItems:       state.GetLifecycle().GetMaxItems(),
		MaxBytes:       state.GetLifecycle().GetMaxBytes(),
		BudgetMaxItems: state.GetBudget().GetMaxItems(),
		BudgetMaxBytes: state.GetBudget().GetMaxBytes(),
	}
	if state.GetCloseTime() != nil {
		info.CloseTime = state.GetCloseTime().AsTime()
	}
	if owner := target.owner; owner != nil {
		info.RunId, info.Name = owner.GetRunId(), target.name
		if owner.GetKind() == streampb.STREAM_OWNER_KIND_ACTIVITY {
			info.ActivityId = owner.GetId()
		} else {
			info.WorkflowId, info.ActivityId = owner.GetId(), owner.GetActivityId()
		}
	}
	cctx.Printer.Println(color.MagentaString("Stream:"))
	if err := cctx.Printer.PrintStructured(info, printer.StructuredOptions{}); err != nil {
		return err
	}

	ch := target.channel()
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Notification Channel:"))
	if err := cctx.Printer.PrintStructured(struct {
		Channel  string
		Kind     string
		LinkedTo string `cli:",cardOmitEmpty"`
	}{
		Channel:  ch.name,
		Kind:     ch.kind.String(),
		LinkedTo: executionText(ch.owner),
	}, printer.StructuredOptions{}); err != nil {
		return err
	}

	type producerRow struct {
		ProducerId  string
		Sequence    int64
		FirstOffset int64
		Count       int64
		Fenced      bool
	}
	producers := make([]producerRow, 0, len(state.GetProducers()))
	for id, p := range state.GetProducers() {
		producers = append(producers, producerRow{
			ProducerId:  id,
			Sequence:    p.GetSeq(),
			FirstOffset: p.GetFirstOffset(),
			Count:       p.GetCount(),
			Fenced:      p.GetFenced(),
		})
	}
	sort.Slice(producers, func(i, j int) bool {
		return producers[i].ProducerId < producers[j].ProducerId
	})
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Producers: %v", len(producers)))
	if len(producers) > 0 {
		if err := cctx.Printer.PrintStructured(producers, printer.StructuredOptions{
			Table: &printer.TableOptions{},
		}); err != nil {
			return err
		}
	}

	type consumerRow struct {
		ConsumerId  string
		WorkflowId  string
		RunId       string
		Offset      int64
		ReplayFloor int64
		Active      bool
		External    bool
	}
	consumers := make([]consumerRow, 0, len(state.GetConsumers()))
	for id, c := range state.GetConsumers() {
		consumers = append(consumers, consumerRow{
			ConsumerId:  id,
			WorkflowId:  c.GetWorkflowId(),
			RunId:       c.GetRunId(),
			Offset:      c.GetOffset(),
			ReplayFloor: c.GetReplayFloor(),
			Active:      c.GetActive(),
			External:    c.GetExternal(),
		})
	}
	sort.Slice(consumers, func(i, j int) bool {
		return consumers[i].ConsumerId < consumers[j].ConsumerId
	})
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Consumers: %v", len(consumers)))
	if len(consumers) > 0 {
		return cctx.Printer.PrintStructured(consumers, printer.StructuredOptions{
			Table: &printer.TableOptions{},
		})
	}
	return nil
}

// startPosition settles where the first poll begins. The beginning is the
// default; an explicit offset of zero is allowed and differs from it once the
// floor has moved.
func (c *TemporalStreamReadCommand) startPosition() (*streamapi.StreamStartPosition, error) {
	fromOffset := c.Command.Flags().Changed("from-offset")
	set := 0
	for _, on := range []bool{fromOffset, c.FromTail, c.Last != 0} {
		if on {
			set++
		}
	}
	if set > 1 {
		return nil, fmt.Errorf("set at most one of --from-offset, --from-tail and --last")
	}
	switch {
	case fromOffset:
		if c.FromOffset < 0 {
			return nil, fmt.Errorf("--from-offset cannot be negative")
		}
		return &streamapi.StreamStartPosition{
			Position: &streamapi.StreamStartPosition_Offset{Offset: int64(c.FromOffset)},
		}, nil
	case c.FromTail:
		return &streamapi.StreamStartPosition{
			Position: &streamapi.StreamStartPosition_Tail{Tail: true},
		}, nil
	case c.Last != 0:
		if c.Last < 0 {
			return nil, fmt.Errorf("--last must be positive")
		}
		return &streamapi.StreamStartPosition{
			Position: &streamapi.StreamStartPosition_LastN{LastN: int64(c.Last)},
		}, nil
	default:
		return &streamapi.StreamStartPosition{
			Position: &streamapi.StreamStartPosition_Earliest{Earliest: true},
		}, nil
	}
}

// streamReader pages through a stream. Without follow it stops once caught up
// with the frontier; with follow it long-polls until the stream closes.
type streamReader struct {
	ctx      context.Context
	cl       *streamClient
	target   streamTarget
	start    *streamapi.StreamStartPosition
	from     int64
	topics   []string
	follow   bool
	pageSize int32
	limit    int
	seen     int
	buf      []*streampb.StreamRecord
	done     bool
}

func (r *streamReader) next() (*streampb.StreamRecord, error) {
	if r.limit > 0 && r.seen >= r.limit {
		return nil, nil
	}
	for len(r.buf) == 0 && !r.done {
		if err := r.poll(); err != nil {
			// An interrupted command is the reader leaving, not a failure.
			if r.ctx.Err() != nil {
				r.done = true
				break
			}
			return nil, fmt.Errorf("failed reading %v: %w", r.target, plainRefusal(err))
		}
	}
	if len(r.buf) == 0 {
		return nil, nil
	}
	rec := r.buf[0]
	r.buf = r.buf[1:]
	r.seen++
	return rec, nil
}

func (r *streamReader) poll() error {
	out, err := r.cl.poll(r.ctx, r.target, r.from, r.start, r.pageSize, r.topics, r.follow)
	if err != nil {
		return err
	}
	// The first poll resolved the start; from here the cursor is an offset.
	r.start = nil
	r.from = out.GetNextOffset()
	if err := r.cl.decodeRecords(out.GetRecords()); err != nil {
		return fmt.Errorf("decoding records: %w", err)
	}
	r.buf = append(r.buf, out.GetRecords()...)
	caughtUp := out.GetNextOffset() >= out.GetHeadOffset()
	if caughtUp && (out.GetClosed() || !r.follow) {
		r.done = true
	}
	return nil
}

type streamRecordRow struct {
	Offset   int64
	Kind     string
	Topic    string
	Producer string
	Attempt  int64
	Sequence int64
	Body     string
}

// streamRowIter adapts the reader to the printer's streaming table.
type streamRowIter struct {
	cctx   *CommandContext
	reader *streamReader
}

func (i *streamRowIter) Next() (any, error) {
	rec, err := i.reader.next()
	if rec == nil || err != nil {
		return nil, err
	}
	body, err := payloadText(rec.GetBody())
	if err != nil {
		return nil, err
	}
	return streamRecordRow{
		Offset:   rec.GetOffset(),
		Kind:     recordKindText(rec.GetKind()),
		Topic:    rec.GetTopic(),
		Producer: rec.GetProducerId(),
		Attempt:  rec.GetAttempt(),
		Sequence: rec.GetSequence(),
		Body:     body,
	}, nil
}

// recordKindText is the kind in the shorthand the API's enums print with, the
// way every other enum in a table reads.
func recordKindText(kind streamapi.StreamRecordKind) string {
	if kind == streamapi.STREAM_RECORD_KIND_UNSPECIFIED {
		kind = streamapi.STREAM_RECORD_KIND_DATA
	}
	return kind.String()
}

// payloadText renders a payload for a table cell the way the printer renders
// payloads elsewhere: shorthand, on one line.
func payloadText(p *commonpb.Payload) (string, error) {
	if p == nil {
		return "", nil
	}
	b, err := temporalproto.CustomJSONMarshalOptions{
		Metadata: map[string]any{commonpb.EnablePayloadShorthandMetadataKey: true},
	}.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("failed rendering payload: %w", err)
	}
	return string(b), nil
}

func (c *TemporalStreamReadCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	start, err := c.startPosition()
	if err != nil {
		return err
	}
	if c.PageSize <= 0 {
		return fmt.Errorf("--page-size must be positive")
	}
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	reader := &streamReader{
		ctx:      cctx,
		cl:       cl,
		target:   target,
		start:    start,
		topics:   c.Topic,
		follow:   c.Follow,
		pageSize: int32(c.PageSize),
		limit:    c.Limit,
	}
	if cctx.JSONOutput {
		// This is a listing command subject to json vs jsonl rules
		cctx.Printer.StartList()
		defer cctx.Printer.EndList()
		for {
			rec, err := reader.next()
			if err != nil {
				return err
			}
			if rec == nil {
				return nil
			}
			if err := cctx.Printer.PrintStructured(rec, printer.StructuredOptions{}); err != nil {
				return err
			}
		}
	}
	return cctx.Printer.PrintStructuredTableIter(
		reflect.TypeOf(streamRecordRow{}),
		&streamRowIter{cctx: cctx, reader: reader},
		printer.StructuredOptions{
			Table: &printer.TableOptions{
				// Streaming rows cannot be measured first, so the columns
				// before the body take fixed widths.
				FieldWidths: map[string]int{
					"Offset":   8,
					"Kind":     8,
					"Topic":    16,
					"Producer": 20,
					"Attempt":  8,
					"Sequence": 10,
				},
			},
		},
	)
}

// inputPayloads takes the records from --input and --input-file, or one per
// line of stdin when neither is given.
func (c *TemporalStreamAppendCommand) inputPayloads(
	cctx *CommandContext,
) ([]*commonpb.Payload, error) {
	if len(c.Input) == 0 && len(c.InputFile) == 0 {
		lines, err := readStdinLines(cctx)
		if err != nil {
			return nil, err
		}
		c.Input = lines
	}
	if len(c.Input) == 0 && len(c.InputFile) == 0 {
		return nil, nil
	}
	payloads, err := c.buildRawInputPayloads()
	if err != nil {
		return nil, err
	}
	return payloads.GetPayloads(), nil
}

func readStdinLines(cctx *CommandContext) ([]string, error) {
	// A terminal with nothing piped in would sit waiting for input the user
	// did not mean to type.
	if f, ok := cctx.Options.Stdin.(*os.File); ok && isatty.IsTerminal(f.Fd()) {
		return nil, nil
	}
	scanner := bufio.NewScanner(cctx.Options.Stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 16<<20)
	var lines []string
	for scanner.Scan() {
		if line := strings.TrimSpace(scanner.Text()); line != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed reading stdin: %w", err)
	}
	return lines, nil
}

func (c *TemporalStreamAppendCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	var expectedOffset *int64
	if c.Command.Flags().Changed("expected-offset") {
		if target.streamID == "" {
			return fmt.Errorf("--expected-offset applies to a standalone stream")
		}
		if c.ExpectedOffset < 0 {
			return fmt.Errorf("--expected-offset cannot be negative")
		}
		v := int64(c.ExpectedOffset)
		expectedOffset = &v
	}
	if c.Attempt < 0 || c.Sequence < 0 {
		return fmt.Errorf("--attempt and --sequence cannot be negative")
	}
	sequenceSet := c.Command.Flags().Changed("sequence")
	if sequenceSet && c.ProducerId == "" {
		return fmt.Errorf("--sequence needs --producer-id")
	}
	payloads, err := c.inputPayloads(cctx)
	if err != nil {
		return err
	}
	if len(payloads) == 0 && !c.Finish {
		return fmt.Errorf(
			"no records to append: pass --input, --input-file, lines on stdin, or --finish")
	}

	records := make([]*streampb.StreamRecord, 0, len(payloads)+1)
	for i, p := range payloads {
		rec := &streampb.StreamRecord{
			Body:       p,
			Topic:      c.Topic,
			Kind:       streamapi.STREAM_RECORD_KIND_DATA,
			ProducerId: c.ProducerId,
			Attempt:    int64(c.Attempt),
		}
		if sequenceSet {
			rec.Sequence = int64(c.Sequence + i)
		}
		records = append(records, rec)
	}
	if c.Finish {
		rec := &streampb.StreamRecord{
			Topic:      c.Topic,
			Kind:       streamapi.STREAM_RECORD_KIND_FINISH,
			ProducerId: c.ProducerId,
			Attempt:    int64(c.Attempt),
		}
		if sequenceSet {
			rec.Sequence = int64(c.Sequence + len(payloads))
		}
		records = append(records, rec)
	}

	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	if err := cl.encodeRecords(records); err != nil {
		return fmt.Errorf("failed encoding records: %w", err)
	}
	var sequence int64
	if sequenceSet {
		sequence = int64(c.Sequence)
	}
	out, err := cl.append(cctx, target, records, c.ProducerId, sequence, expectedOffset)
	if err != nil {
		return fmt.Errorf("failed appending to %v: %w", target, plainRefusal(err))
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(out, printer.StructuredOptions{})
	}
	return cctx.Printer.PrintStructured(struct {
		FirstOffset  int64
		NextOffset   int64
		Count        int64
		Deduplicated bool
	}{
		FirstOffset:  out.GetFirstOffset(),
		NextOffset:   out.GetNextOffset(),
		Count:        out.GetCount(),
		Deduplicated: out.GetDeduplicated(),
	}, printer.StructuredOptions{})
}

func (c *TemporalStreamCloseCommand) run(cctx *CommandContext, _ []string) error {
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	var reason *commonpb.Payload
	if c.Reason != "" {
		reason, err = converter.GetDefaultDataConverter().ToPayload(c.Reason)
		if err != nil {
			return fmt.Errorf("failed converting reason: %w", err)
		}
		if cl.PayloadCodec != nil {
			encoded, err := cl.PayloadCodec.Encode([]*commonpb.Payload{reason})
			if err != nil {
				return fmt.Errorf("failed encoding reason: %w", err)
			}
			reason = encoded[0]
		}
	}
	_, err = cl.svc.CloseStream(cctx, &streampb.CloseStreamRequest{
		FrontendRequest: &streampb.CloseStreamInput{
			Namespace: cl.namespace,
			StreamId:  c.StreamId,
			Reason:    reason,
		},
	})
	if err != nil {
		return fmt.Errorf("failed closing stream %q: %w", c.StreamId, err)
	}
	cctx.Printer.Printlnf("Closed stream %v", c.StreamId)
	return nil
}

func (c *TemporalStreamTruncateCommand) run(cctx *CommandContext, _ []string) error {
	if c.To < 0 {
		return fmt.Errorf("--to cannot be negative")
	}
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	_, err = cl.svc.TruncateStream(cctx, &streampb.TruncateStreamRequest{
		FrontendRequest: &streampb.TruncateStreamInput{
			Namespace:     cl.namespace,
			StreamId:      c.StreamId,
			NewBaseOffset: int64(c.To),
		},
	})
	if err != nil {
		return fmt.Errorf("failed truncating stream %q: %w", c.StreamId, err)
	}
	cctx.Printer.Printlnf("Truncated stream %v to offset %v", c.StreamId, c.To)
	return nil
}

func (c *TemporalStreamDeleteCommand) run(cctx *CommandContext, _ []string) error {
	cl, err := dialStreamClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	_, err = cl.svc.DeleteStream(cctx, &streampb.DeleteStreamRequest{
		FrontendRequest: &streampb.DeleteStreamInput{
			Namespace: cl.namespace,
			StreamId:  c.StreamId,
			Force:     c.Force,
		},
	})
	if err != nil {
		return fmt.Errorf("failed deleting stream %q: %w", c.StreamId, err)
	}
	cctx.Printer.Printlnf("Deleted stream %v", c.StreamId)
	return nil
}
