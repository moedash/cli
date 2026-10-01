package temporalcli_test

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/temporalio/cli/internal/temporalcli"
	streamapi "go.temporal.io/api/stream/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	streampb "go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
)

func (s *SharedServerSuite) createStream(id string, args ...string) {
	res := s.Execute(append([]string{
		"stream", "create", "--address", s.Address(), "--stream-id", id,
	}, args...)...)
	s.NoError(res.Err)
	s.Contains(res.Stdout.String(), id)
}

// readStreamJSON runs a read with JSON output and decodes every record.
func (s *SharedServerSuite) readStreamJSON(args ...string) []*streampb.StreamRecord {
	res := s.Execute(append([]string{
		"stream", "read", "--address", s.Address(), "-o", "json",
	}, args...)...)
	s.NoError(res.Err)
	var raw []json.RawMessage
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &raw))
	records := make([]*streampb.StreamRecord, len(raw))
	for i, r := range raw {
		records[i] = &streampb.StreamRecord{}
		s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(r, records[i], true))
	}
	return records
}

func (s *SharedServerSuite) TestStream_StandaloneLifecycle() {
	id := "stream-" + uuid.NewString()
	s.createStream(id, "--retention", "1h", "--max-bytes", "1048576")

	// The same lifecycle again is a retry; a different one is refused.
	res := s.Execute(
		"stream", "create", "--address", s.Address(), "--stream-id", id,
		"--retention", "1h", "--max-bytes", "1048576",
	)
	s.ErrorContains(res.Err, "already exists")
	res = s.Execute(
		"stream", "create", "--address", s.Address(), "--stream-id", id,
		"--retention", "1h", "--max-bytes", "2048",
	)
	s.ErrorContains(res.Err, "different lifecycle")
	s.NotContains(res.Err.Error(), "STREAM_POLICY_MISMATCH")

	appendArgs := []string{
		"stream", "append", "--address", s.Address(), "--stream-id", id,
		"--topic", "scores", "--producer-id", "p1", "--attempt", "1", "--sequence", "1",
		"--input", `{"home": 1}`, "--input", `{"home": 2}`,
	}
	res = s.Execute(appendArgs...)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "FirstOffset", "0")
	s.ContainsOnSameLine(res.Stdout.String(), "NextOffset", "2")
	s.ContainsOnSameLine(res.Stdout.String(), "Deduplicated", "false")

	// The same producer retrying the same sequence appends nothing.
	res = s.Execute(appendArgs...)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Deduplicated", "true")

	// Different content at that sequence is a conflict, and an older sequence
	// is stale; both are refused in plain words, without the token.
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id,
		"--producer-id", "p1", "--sequence", "1", "--input", `{"home": 9}`,
	)
	s.ErrorContains(res.Err, "different content at this sequence")
	s.NotContains(res.Err.Error(), "STREAM_PRODUCER_CONFLICT")
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id,
		"--producer-id", "p1", "--sequence", "0", "--input", `{"home": 9}`,
	)
	s.ErrorContains(res.Err, "below the producer's latest")
	s.NotContains(res.Err.Error(), "STREAM_PRODUCER_STALE_SEQUENCE")

	// One record per stdin line, then the producer's FINISH.
	s.Stdin.WriteString("\"third\"\n\n\"fourth\"\n")
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id,
		"--producer-id", "p2", "--finish",
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "FirstOffset", "2")
	s.ContainsOnSameLine(res.Stdout.String(), "Count", "3")

	// Nothing to append is an error rather than an empty batch.
	res = s.Execute("stream", "append", "--address", s.Address(), "--stream-id", id)
	s.ErrorContains(res.Err, "no records to append")

	// Read from the beginning as a table.
	res = s.Execute("stream", "read", "--address", s.Address(), "--stream-id", id)
	s.NoError(res.Err)
	out := res.Stdout.String()
	s.ContainsOnSameLine(out, "Offset", "Kind", "Topic", "Producer", "Attempt", "Sequence", "Body")
	s.ContainsOnSameLine(out, "0", "Data", "scores", "p1", "1", "1", `{"home":1}`)
	s.ContainsOnSameLine(out, "1", "Data", "scores", "p1", "1", "2", `{"home":2}`)
	s.ContainsOnSameLine(out, "2", "Data", "p2", `"third"`)
	s.ContainsOnSameLine(out, "3", "Data", "p2", `"fourth"`)
	s.ContainsOnSameLine(out, "4", "Finish", "p2")

	// The same records as JSON, with the body in shorthand.
	records := s.readStreamJSON("--stream-id", id)
	s.Len(records, 5)
	s.Equal(int64(3), records[3].GetOffset())
	s.Equal(`"fourth"`, string(records[3].GetBody().GetData()))
	s.Equal("p2", records[4].GetProducerId())
	s.Equal(streamapi.STREAM_RECORD_KIND_FINISH, records[4].GetKind())

	// Start positions and filters.
	records = s.readStreamJSON("--stream-id", id, "--last", "2")
	s.Len(records, 2)
	s.Equal(int64(3), records[0].GetOffset())
	records = s.readStreamJSON("--stream-id", id, "--from-offset", "4")
	s.Len(records, 1)
	s.Equal(int64(4), records[0].GetOffset())
	records = s.readStreamJSON("--stream-id", id, "--topic", "scores")
	s.Len(records, 2)
	records = s.readStreamJSON("--stream-id", id, "--limit", "3", "--page-size", "2")
	s.Len(records, 3)
	records = s.readStreamJSON("--stream-id", id, "--from-tail")
	s.Empty(records)

	// Describe shows the frontier and both producers.
	res = s.Execute("stream", "describe", "--address", s.Address(), "--stream-id", id)
	s.NoError(res.Err)
	out = res.Stdout.String()
	s.ContainsOnSameLine(out, "StreamId", id)
	s.ContainsOnSameLine(out, "HeadOffset", "5")
	s.ContainsOnSameLine(out, "Retention", "1h")
	s.ContainsOnSameLine(out, "MaxBytes", "1048576")
	s.ContainsOnSameLine(out, "HeldBytes")
	s.ContainsOnSameLine(out, "Producers", "2")
	s.ContainsOnSameLine(out, "p1", "1", "0", "2", "false")
	// A FINISH record tells readers the producer is done; only FinishWriting
	// fences it on the stream.
	s.ContainsOnSameLine(out, "p2", "0", "2", "3", "false")
	s.ContainsOnSameLine(out, "Closed", "false")
	s.ContainsOnSameLine(out, "Consumers", "0")

	res = s.Execute("stream", "describe", "--address", s.Address(), "--stream-id", id, "-o", "json")
	s.NoError(res.Err)
	var state streampb.StreamState
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &state, true))
	s.Equal(int64(5), state.GetHeadOffset())
	s.Equal(int64(0), state.GetBaseOffset())
	s.False(state.GetClosed())
	s.Equal(int64(3), state.GetProducers()["p2"].GetCount())
	s.Equal(int64(1048576), state.GetLifecycle().GetMaxBytes())
	s.Positive(state.GetHeldBytes())
	s.Equal(state.GetAppendedBytes(), state.GetHeldBytes())

	// Truncation moves the floor; the beginning is now the oldest record left,
	// and the reclaimed batch no longer counts as held.
	res = s.Execute("stream", "truncate", "--address", s.Address(), "--stream-id", id, "--to", "2")
	s.NoError(res.Err)
	records = s.readStreamJSON("--stream-id", id)
	s.Len(records, 3)
	s.Equal(int64(2), records[0].GetOffset())
	res = s.Execute("stream", "describe", "--address", s.Address(), "--stream-id", id, "-o", "json")
	s.NoError(res.Err)
	var truncated streampb.StreamState
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(res.Stdout.Bytes(), &truncated, true))
	s.Equal(int64(2), truncated.GetBaseOffset())
	s.Less(truncated.GetHeldBytes(), truncated.GetAppendedBytes())
	res = s.Execute(
		"stream", "read", "--address", s.Address(), "--stream-id", id, "--from-offset", "0",
	)
	s.ErrorContains(res.Err, "below the stream's floor")
	s.NotContains(res.Err.Error(), "STREAM_CURSOR_BELOW_FLOOR")

	// Closing seals the stream but keeps it readable.
	res = s.Execute("stream", "close", "--address", s.Address(), "--stream-id", id, "--reason", "done")
	s.NoError(res.Err)
	res = s.Execute("stream", "describe", "--address", s.Address(), "--stream-id", id)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "Closed", "true")
	s.ContainsOnSameLine(res.Stdout.String(), "CloseReason", `"done"`)
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id, "--input", `"late"`,
	)
	s.ErrorContains(res.Err, "the stream is closed")
	s.NotContains(res.Err.Error(), "STREAM_CLOSED")
	s.Len(s.readStreamJSON("--stream-id", id), 3)

	// Deleting takes the records with it.
	res = s.Execute("stream", "delete", "--address", s.Address(), "--stream-id", id)
	s.NoError(res.Err)
	res = s.Execute("stream", "describe", "--address", s.Address(), "--stream-id", id)
	s.Error(res.Err)
}

func (s *SharedServerSuite) TestStream_ReadFollow() {
	id := "stream-" + uuid.NewString()
	s.createStream(id)
	res := s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id, "--input", `"first"`,
	)
	s.NoError(res.Err)

	done := make(chan *CommandResult, 1)
	go func() {
		done <- s.Execute("stream", "read", "--address", s.Address(), "--stream-id", id, "--follow")
	}()

	// The follow is parked on the frontier by the time this lands.
	time.Sleep(time.Second)
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--stream-id", id, "--input", `"second"`,
	)
	s.NoError(res.Err)
	res = s.Execute("stream", "close", "--address", s.Address(), "--stream-id", id)
	s.NoError(res.Err)

	select {
	case res = <-done:
	case <-time.After(60 * time.Second):
		s.Fail("follow did not end after the stream closed")
	}
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "0", "Data", `"first"`)
	s.ContainsOnSameLine(res.Stdout.String(), "1", "Data", `"second"`)
}

func (s *SharedServerSuite) TestStream_OwnedByWorkflow() {
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

	// An owned stream comes into being on its first append.
	res := s.Execute(
		"stream", "append", "--address", s.Address(),
		"--workflow-id", run.GetID(), "--name", "scores",
		"--producer-id", "outside", "--input", `{"home": 1}`, "--input", `{"home": 2}`,
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "NextOffset", "2")

	res = s.Execute(
		"stream", "read", "--address", s.Address(),
		"--workflow-id", run.GetID(), "--run-id", run.GetRunID(), "--name", "scores",
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "0", "Data", "outside", `{"home":1}`)
	s.ContainsOnSameLine(res.Stdout.String(), "1", "Data", "outside", `{"home":2}`)

	res = s.Execute(
		"stream", "describe", "--address", s.Address(), "--workflow-id", run.GetID(), "--name", "scores",
	)
	s.NoError(res.Err)
	s.ContainsOnSameLine(res.Stdout.String(), "WorkflowId", run.GetID())
	s.ContainsOnSameLine(res.Stdout.String(), "Name", "scores")
	s.ContainsOnSameLine(res.Stdout.String(), "HeadOffset", "2")

	// The owner's default stream is a different, still empty, stream.
	s.Empty(s.readStreamJSON("--workflow-id", run.GetID()))

	s.NoError(s.Client.SignalWorkflow(s.Context, run.GetID(), "", "finish", nil))
	s.NoError(run.Get(s.Context, nil))
}

func (s *SharedServerSuite) TestStream_List() {
	prefix := "list-" + uuid.NewString()[:8]
	s.createStream(prefix + "-a")
	s.createStream(prefix + "-b")
	other := "other-" + uuid.NewString()
	s.createStream(other)
	query := fmt.Sprintf(`WorkflowId STARTS_WITH "%s"`, prefix)

	var out string
	s.EventuallyWithT(func(t *assert.CollectT) {
		res := s.Execute("stream", "list", "--address", s.Address(), "--query", query)
		assert.NoError(t, res.Err)
		out = res.Stdout.String()
		assert.Contains(t, out, prefix+"-a")
		assert.Contains(t, out, prefix+"-b")
	}, 10*time.Second, 200*time.Millisecond)
	s.ContainsOnSameLine(out, "StreamId", "RunId")
	s.NotContains(out, other)

	res := s.Execute(
		"stream", "list", "--address", s.Address(), "--query", query, "--limit", "1", "-o", "json",
	)
	s.NoError(res.Err)
	var entries []json.RawMessage
	s.NoError(json.Unmarshal(res.Stdout.Bytes(), &entries))
	s.Len(entries, 1)
	var entry streampb.StreamListEntry
	s.NoError(temporalcli.UnmarshalProtoJSONWithOptions(entries[0], &entry, true))
	s.Contains(entry.GetStreamId(), prefix)
	s.NotEmpty(entry.GetRunId())
}

func (s *SharedServerSuite) TestStream_ReferenceValidation() {
	res := s.Execute(
		"stream", "read", "--address", s.Address(), "--stream-id", "a", "--workflow-id", "b",
	)
	s.ErrorContains(res.Err, "cannot set --stream-id")
	res = s.Execute("stream", "read", "--address", s.Address())
	s.ErrorContains(res.Err, "must set either --stream-id")
	res = s.Execute("stream", "read", "--address", s.Address(), "--stream-id", "a", "--name", "n")
	s.ErrorContains(res.Err, "--run-id and --name")
	res = s.Execute(
		"stream", "read", "--address", s.Address(), "--stream-id", "a", "--from-tail", "--last", "2",
	)
	s.ErrorContains(res.Err, "at most one")
	res = s.Execute(
		"stream", "append", "--address", s.Address(), "--workflow-id", "w",
		"--expected-offset", "3", "--input", "1",
	)
	s.ErrorContains(res.Err, "--expected-offset applies to a standalone stream")
	res = s.Execute("stream", "truncate", "--address", s.Address(), "--to", "1")
	s.ErrorContains(res.Err, "stream-id")
	res = s.Execute(
		"stream", "create", "--address", s.Address(), "--stream-id", "a", "--max-bytes", "-1",
	)
	s.ErrorContains(res.Err, "cannot be negative")
}
