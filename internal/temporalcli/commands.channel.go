package temporalcli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/temporalio/cli/internal/printer"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notification/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"google.golang.org/protobuf/types/known/durationpb"
)

// channelPollGrace is how long past the requested wait a poll may take before
// the CLI gives up on it, so a Service answering at its own deadline is not cut
// off.
const channelPollGrace = 10 * time.Second

// channelTarget is the channel a command names: an independent channel by its
// name alone, or a channel linked to an execution by that owner and the name.
type channelTarget struct {
	name      string
	execution *commonpb.Execution
}

func (o *ChannelOptions) target() (channelTarget, error) {
	if o.WorkflowId != "" && o.ActivityId != "" {
		return channelTarget{}, fmt.Errorf(
			"--workflow-id and --activity-id name different owners, set one of them")
	}
	if o.RunId != "" && o.WorkflowId == "" && o.ActivityId == "" {
		return channelTarget{}, fmt.Errorf("--run-id requires --workflow-id or --activity-id")
	}
	t := channelTarget{name: o.Channel}
	switch {
	case o.WorkflowId != "":
		t.execution = &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
			BusinessId: o.WorkflowId,
			RunId:      o.RunId,
		}
	case o.ActivityId != "":
		t.execution = &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
			BusinessId: o.ActivityId,
			RunId:      o.RunId,
		}
	}
	return t, nil
}

// executionKindText is the owner's kind the way the flags name it, so output
// reads back as the flag that reaches the same owner.
func executionKindText(e *commonpb.Execution) string {
	switch e.GetType() {
	case enumspb.EXECUTION_TYPE_ACTIVITY:
		return "activity"
	case enumspb.EXECUTION_TYPE_NEXUS_OPERATION:
		return "nexus operation"
	default:
		return "workflow"
	}
}

// executionText names an execution for a card or a message: its kind and ID,
// then the run when one was given.
func executionText(e *commonpb.Execution) string {
	if e == nil {
		return ""
	}
	text := executionKindText(e) + " " + e.GetBusinessId()
	if e.GetRunId() != "" {
		text += " (run " + e.GetRunId() + ")"
	}
	return text
}

func (t channelTarget) String() string {
	if t.execution == nil {
		return fmt.Sprintf("channel %q", t.name)
	}
	return fmt.Sprintf("channel %q of %s %q", t.name, executionKindText(t.execution),
		t.execution.GetBusinessId())
}

// label is the target for a line of normal output, where quotes would be noise.
func (t channelTarget) label() string {
	if t.execution == nil {
		return "channel " + t.name
	}
	return "channel " + t.name + " of " + executionKindText(t.execution) + " " +
		t.execution.GetBusinessId()
}

// plainChannelRefusal turns the Service's refusals into what to do next. Every
// other error comes back as it was.
func plainChannelRefusal(err error, t channelTarget) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		plain := fmt.Sprintf("there is no channel %q. A channel exists once a writer "+
			"notifies it or a listener registers on it, and goes away after a while "+
			"with neither.", t.name)
		if t.execution != nil {
			plain = fmt.Sprintf("%s %q has no running execution to reach. A linked "+
				"channel lives only while its owner runs.",
				executionKindText(t.execution), t.execution.GetBusinessId())
		}
		return &refusalError{plain: plain, detail: notFound.Message, cause: err}
	}
	var exhausted *serviceerror.ResourceExhausted
	if !errors.As(err, &exhausted) {
		return err
	}
	var plain string
	switch exhausted.Cause {
	case enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT:
		plain = "the channel already has as many listeners as it may hold, so this one " +
			"was not registered. Remove a listener it no longer needs, or use another channel."
	case enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT:
		plain = "the namespace is notifying channels faster than it may, so this " +
			"notification was not sent. Retry after a moment, or notify less often."
	default:
		plain = "the Service is over one of its limits and refused the call. Retry after " +
			"a moment."
	}
	return &refusalError{plain: plain, detail: exhausted.Message, cause: err}
}

// parseChannelMetadata reads KEY=VALUE pairs whose values are JSON, carried as
// JSON payloads the way --input carries a Signal's arguments.
func parseChannelMetadata(pairs []string) (map[string]*commonpb.Payload, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	out := make(map[string]*commonpb.Payload, len(pairs))
	for _, pair := range pairs {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("--metadata %q must be KEY=VALUE", pair)
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("--metadata key %q is given more than once", key)
		}
		if !json.Valid([]byte(value)) {
			return nil, fmt.Errorf("--metadata value for %q is not valid JSON", key)
		}
		out[key] = &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     []byte(value),
		}
	}
	return out, nil
}

// positionText shows a position as the text it most often is, and as base64
// when it holds bytes a terminal would mangle.
func positionText(p []byte) string {
	if utf8.Valid(p) && strings.IndexFunc(string(p), func(r rune) bool {
		return !unicode.IsPrint(r)
	}) < 0 {
		return string(p)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(p)
}

// metadataText puts a notification's metadata on one line, keys sorted so the
// same notification always reads the same.
func metadataText(md map[string]*commonpb.Payload) (string, error) {
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		v, err := payloadText(md[k])
		if err != nil {
			return "", err
		}
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, " "), nil
}

type channelNotificationRow struct {
	Counter  int64
	Position string
	Metadata string
}

func notificationRow(n *notificationpb.Notification) (channelNotificationRow, error) {
	md, err := metadataText(n.GetMetadata())
	if err != nil {
		return channelNotificationRow{}, err
	}
	return channelNotificationRow{
		Counter:  n.GetCounter(),
		Position: positionText(n.GetPosition()),
		Metadata: md,
	}, nil
}

// channelKindText reads a server that predates the linked kind, and so leaves
// the kind unset, as the independent channel it serves.
func channelKindText(kind notificationpb.ChannelKind) string {
	if kind == notificationpb.CHANNEL_KIND_UNSPECIFIED {
		kind = notificationpb.CHANNEL_KIND_INDEPENDENT
	}
	return kind.String()
}

// printChannelSubscriptions lists the channels a workflow stands on, as its
// description reports them. A workflow that stands on none prints nothing, so
// the section is absent on servers that predate it as well.
func printChannelSubscriptions(
	cctx *CommandContext, subs []*workflowpb.ChannelSubscriptionInfo,
) error {
	if len(subs) == 0 {
		return nil
	}
	rows := make([]struct {
		Channel          string
		Kind             string
		LastCounter      int64
		PendingCounter   string
		ScheduledCounter int64
		Listeners        int32
		Retained         int32
	}, len(subs))
	for i, s := range subs {
		rows[i].Channel = s.GetChannel()
		rows[i].Kind = channelKindText(s.GetKind())
		rows[i].LastCounter = s.GetLastCounter()
		if pending := s.GetPendingNotification(); pending != nil {
			rows[i].PendingCounter = strconv.FormatInt(pending.GetCounter(), 10)
		}
		rows[i].ScheduledCounter = s.GetScheduledCounter()
		rows[i].Listeners = s.GetListenerCount()
		rows[i].Retained = s.GetRetainedCount()
	}
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Notification Channels: %v", len(subs)))
	cctx.Printer.Println()
	return cctx.Printer.PrintStructured(rows, printer.StructuredOptions{
		Table: &printer.TableOptions{},
	})
}

func (c *TemporalChannelNotifyCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	if c.Counter <= 0 {
		return fmt.Errorf("--counter must be greater than zero")
	}
	metadata, err := parseChannelMetadata(c.Metadata)
	if err != nil {
		return err
	}
	cl, err := dialClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	resp, err := cl.WorkflowService().NotifyChannel(cctx, &workflowservice.NotifyChannelRequest{
		Namespace: c.Parent.Namespace,
		Notification: &notificationpb.Notification{
			Channel:  c.Channel,
			Position: []byte(c.Position),
			Counter:  int64(c.Counter),
			Metadata: metadata,
		},
		Identity:  c.Parent.Identity,
		RequestId: uuid.NewString(),
		Execution: target.execution,
	})
	if err != nil {
		return fmt.Errorf("failed notifying %v: %w", target, plainChannelRefusal(err, target))
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(resp, printer.StructuredOptions{})
	}
	cctx.Printer.Printlnf("Notified %v. Listeners reached: %v",
		target.label(), resp.GetListenerCount())
	return nil
}

func (c *TemporalChannelDescribeCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	cl, err := dialClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	resp, err := cl.WorkflowService().DescribeChannel(cctx,
		&workflowservice.DescribeChannelRequest{
			Namespace: c.Parent.Namespace,
			Channel:   c.Channel,
			Execution: target.execution,
		})
	if err != nil {
		return fmt.Errorf("failed describing %v: %w", target, plainChannelRefusal(err, target))
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(resp, printer.StructuredOptions{})
	}

	info := struct {
		Channel        string
		Kind           string
		LinkedTo       string `cli:",cardOmitEmpty"`
		RetainedCount  int32
		LatestCounter  int64  `cli:",cardOmitEmpty"`
		LatestPosition string `cli:",cardOmitEmpty"`
		LatestMetadata string `cli:",cardOmitEmpty"`
	}{
		Channel:       c.Channel,
		Kind:          channelKindText(resp.GetKind()),
		LinkedTo:      executionText(resp.GetLinkedTo()),
		RetainedCount: resp.GetRetainedCount(),
	}
	if latest := resp.GetLatest(); latest != nil {
		row, err := notificationRow(latest)
		if err != nil {
			return err
		}
		info.LatestCounter, info.LatestPosition, info.LatestMetadata =
			row.Counter, row.Position, row.Metadata
	}
	cctx.Printer.Println(color.MagentaString("Channel:"))
	if err := cctx.Printer.PrintStructured(info, printer.StructuredOptions{}); err != nil {
		return err
	}

	type workflowRow struct {
		ListenerId string
		WorkflowId string
		RunId      string
		Registered time.Time
	}
	// Headers stay out of the table because they often carry credentials.
	type callbackRow struct {
		ListenerId string
		Url        string
		Registered time.Time
	}
	var workflows []workflowRow
	var callbacks []callbackRow
	for _, l := range resp.GetListeners() {
		// A linked channel's owner never registered, so it has no time; the
		// conversion would turn that into the Unix epoch.
		var registered time.Time
		if l.GetRegisteredTime() != nil {
			registered = l.GetRegisteredTime().AsTime()
		}
		if wf := l.GetWorkflow(); wf != nil {
			workflows = append(workflows, workflowRow{
				ListenerId: l.GetListenerId(),
				WorkflowId: wf.GetWorkflowId(),
				RunId:      wf.GetRunId(),
				Registered: registered,
			})
		} else if cb := l.GetCallback(); cb != nil {
			callbacks = append(callbacks, callbackRow{
				ListenerId: l.GetListenerId(),
				Url:        cb.GetNexus().GetUrl(),
				Registered: registered,
			})
		}
	}
	sort.Slice(workflows, func(i, j int) bool {
		return workflows[i].ListenerId < workflows[j].ListenerId
	})
	sort.Slice(callbacks, func(i, j int) bool {
		return callbacks[i].ListenerId < callbacks[j].ListenerId
	})
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Workflow listeners: %v", len(workflows)))
	if len(workflows) > 0 {
		if err := cctx.Printer.PrintStructured(workflows, printer.StructuredOptions{
			Table: &printer.TableOptions{},
		}); err != nil {
			return err
		}
	}
	cctx.Printer.Println()
	cctx.Printer.Println(color.MagentaString("Callback listeners: %v", len(callbacks)))
	if len(callbacks) > 0 {
		return cctx.Printer.PrintStructured(callbacks, printer.StructuredOptions{
			Table: &printer.TableOptions{},
		})
	}
	return nil
}

func (c *TemporalChannelListenerAddCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	if c.CallbackUrl == "" {
		return fmt.Errorf("--callback-url cannot be empty")
	}
	headers, err := stringKeysValues(c.Header)
	if err != nil {
		return fmt.Errorf("invalid --header: %w", err)
	}
	clientOpts := &c.Parent.Parent.ClientOptions
	cl, err := dialClient(cctx, clientOpts)
	if err != nil {
		return err
	}
	defer cl.Close()

	resp, err := cl.WorkflowService().RegisterChannelListener(cctx,
		&workflowservice.RegisterChannelListenerRequest{
			Namespace: clientOpts.Namespace,
			Channel:   c.Channel,
			Callback: &commonpb.Callback{
				Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{
					Url:    c.CallbackUrl,
					Header: headers,
				}},
			},
			RequestId: uuid.NewString(),
			Identity:  clientOpts.Identity,
			Execution: target.execution,
		})
	if err != nil {
		return fmt.Errorf("failed adding a listener to %v: %w",
			target, plainChannelRefusal(err, target))
	}
	if cctx.JSONOutput {
		return cctx.Printer.PrintStructured(resp, printer.StructuredOptions{})
	}
	cctx.Printer.Printlnf("Added listener %v to %v", resp.GetListenerId(), target.label())
	return nil
}

func (c *TemporalChannelListenerRemoveCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	clientOpts := &c.Parent.Parent.ClientOptions
	cl, err := dialClient(cctx, clientOpts)
	if err != nil {
		return err
	}
	defer cl.Close()

	_, err = cl.WorkflowService().UnregisterChannelListener(cctx,
		&workflowservice.UnregisterChannelListenerRequest{
			Namespace:  clientOpts.Namespace,
			Channel:    c.Channel,
			ListenerId: c.ListenerId,
			Identity:   clientOpts.Identity,
			Execution:  target.execution,
		})
	if err != nil {
		return fmt.Errorf("failed removing listener %q from %v: %w",
			c.ListenerId, target, plainChannelRefusal(err, target))
	}
	cctx.Printer.Printlnf("Removed listener %v from %v", c.ListenerId, target.label())
	return nil
}

// channelPoller hands out a channel's notifications one at a time. Without
// follow it makes one poll; with follow it polls again from the highest
// counter it has seen until the command is interrupted.
type channelPoller struct {
	ctx       context.Context
	cl        client.Client
	namespace string
	target    channelTarget
	after     int64
	wait      time.Duration
	max       int32
	follow    bool
	buf       []*notificationpb.Notification
	polled    bool
}

func (p *channelPoller) next() (*notificationpb.Notification, error) {
	for len(p.buf) == 0 && (p.follow || !p.polled) {
		if err := p.poll(); err != nil {
			// An interrupted command is the poller leaving, not a failure.
			if p.ctx.Err() != nil {
				return nil, nil
			}
			return nil, fmt.Errorf("failed polling %v: %w",
				p.target, plainChannelRefusal(err, p.target))
		}
	}
	if len(p.buf) == 0 {
		return nil, nil
	}
	n := p.buf[0]
	p.buf = p.buf[1:]
	return n, nil
}

func (p *channelPoller) poll() error {
	ctx, cancel := context.WithTimeout(p.ctx, p.wait+channelPollGrace)
	defer cancel()
	resp, err := p.cl.WorkflowService().PollChannel(ctx, &workflowservice.PollChannelRequest{
		Namespace:        p.namespace,
		Channel:          p.target.name,
		AfterCounter:     p.after,
		Wait:             durationpb.New(p.wait),
		MaxNotifications: p.max,
		Execution:        p.target.execution,
	})
	p.polled = true
	if err != nil {
		// A poll the Service held past its deadline is an empty answer, the same
		// as one it returned with nothing.
		if ctx.Err() != nil && p.ctx.Err() == nil {
			return nil
		}
		return err
	}
	for _, n := range resp.GetNotifications() {
		p.after = max(p.after, n.GetCounter())
	}
	p.buf = append(p.buf, resp.GetNotifications()...)
	return nil
}

// channelRowIter adapts the poller to the printer's streaming table.
type channelRowIter struct{ poller *channelPoller }

func (i *channelRowIter) Next() (any, error) {
	n, err := i.poller.next()
	if n == nil || err != nil {
		return nil, err
	}
	return notificationRow(n)
}

func (c *TemporalChannelPollCommand) run(cctx *CommandContext, _ []string) error {
	target, err := c.target()
	if err != nil {
		return err
	}
	if c.AfterCounter < 0 {
		return fmt.Errorf("--after-counter cannot be negative")
	}
	if c.Max < 0 {
		return fmt.Errorf("--max cannot be negative")
	}
	if c.Wait.Duration() <= 0 {
		return fmt.Errorf("--wait must be positive")
	}
	cl, err := dialClient(cctx, &c.Parent.ClientOptions)
	if err != nil {
		return err
	}
	defer cl.Close()

	poller := &channelPoller{
		ctx:       cctx,
		cl:        cl,
		namespace: c.Parent.Namespace,
		target:    target,
		after:     int64(c.AfterCounter),
		wait:      c.Wait.Duration(),
		max:       int32(c.Max),
		follow:    c.Follow,
	}
	if cctx.JSONOutput {
		// This is a listing command subject to json vs jsonl rules
		cctx.Printer.StartList()
		defer cctx.Printer.EndList()
		for {
			n, err := poller.next()
			if err != nil {
				return err
			}
			if n == nil {
				return nil
			}
			if err := cctx.Printer.PrintStructured(n, printer.StructuredOptions{}); err != nil {
				return err
			}
		}
	}
	if !c.Follow {
		var rows []channelNotificationRow
		for {
			n, err := poller.next()
			if err != nil {
				return err
			}
			if n == nil {
				break
			}
			row, err := notificationRow(n)
			if err != nil {
				return err
			}
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			cctx.Printer.Printlnf("No notifications on %v after counter %v",
				target.label(), c.AfterCounter)
			return nil
		}
		return cctx.Printer.PrintStructured(rows, printer.StructuredOptions{
			Table: &printer.TableOptions{},
		})
	}
	return cctx.Printer.PrintStructuredTableIter(
		reflect.TypeOf(channelNotificationRow{}),
		&channelRowIter{poller: poller},
		printer.StructuredOptions{
			Table: &printer.TableOptions{
				// Streaming rows cannot be measured first, so the columns
				// before the metadata take fixed widths.
				FieldWidths: map[string]int{
					"Counter":  12,
					"Position": 24,
				},
			},
		},
	)
}
