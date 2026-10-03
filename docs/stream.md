# `temporal stream`

Streams are append-only logs the Temporal Service carries next to Workflow
Executions. This fork adds a `temporal stream` command group on top of the
stream service, and a dev server that serves it.

## Dev server

`temporal server start-dev` registers the stream service and turns it on for
every namespace it creates. The server ships with `stream.enabled` off, so the
dev server sets it as one of its own dynamic-config defaults, the same way it
sets the CHASM and standalone-activity flags. An explicit value still wins:

```sh
temporal server start-dev --dynamic-config-value 'stream.enabled=false'
```

The server behind the dev server is pinned in `go.mod` to the stream branch of
`moedash/temporal`, with the matching `moedash/api-go` pin the server needs.

## Addressing a stream

Two kinds of stream exist. A standalone stream has an ID of its own. An owned
stream lives inside a Workflow Execution or an Activity and is named by its
owner and a stream name.

- `--stream-id ID`: the standalone stream `ID`.
- `--workflow-id W [--run-id R] [--name N]`: the stream `N` the workflow owns.
  Without `--name` it is the workflow's default stream.
- `--workflow-id W --activity-id A [--name N]`: the stream of an activity the
  workflow scheduled.
- `--activity-id A [--run-id R] [--name N]`: the stream of a standalone
  activity.

`describe`, `read` and `append` take either form. `create`, `close`,
`truncate` and `delete` act on a standalone stream only, because an owned
stream's lifecycle is its owner's.

## Commands

`stream create --stream-id ID [--retention D] [--max-items N] [--max-bytes N]`
: Creates a standalone stream. Retention defaults to the namespace's; records
  older than it are reclaimed. `--max-items` is a rolling window on records,
  `--max-bytes` a ceiling on held bytes that refuses appends until records
  are reclaimed. Repeating a create with the same lifecycle answers "already
  exists"; a different lifecycle is refused.

`stream list [--query Q] [--limit N] [--page-size N]`
: Lists standalone streams from visibility. `WorkflowId` in the query is the
  stream ID. Owned streams are not listed; reach them through their owner.

`stream describe <ref>`
: Frontier, floor, readable record count, held and appended bytes, close
  state and reason, retention, caps, budget, producers and consumers. Also
  the notification channel the stream notifies on each append and close,
  derived from the reference alone: `stream/NAME` linked to the owner for a
  workflow's or a standalone activity's stream, `stream/ACTIVITY_ID/NAME`
  linked to the workflow for an activity it scheduled, and the independent
  `stream/STREAM_ID` for a standalone stream. The card names the owner as
  `LinkedTo workflow ID` or `LinkedTo activity ID`, with the run when one was
  given. `temporal channel poll --channel C` with the same `--workflow-id` or
  `--activity-id` follows it.

`stream read <ref> [--from-offset N | --from-tail | --last N] [--follow]
[--topic T]... [--limit N]`
: Prints records with offset, kind, topic, producer, attempt, sequence and
  body. Stops once caught up, or with `--follow` when the stream closes.

`stream append <ref> --input V... [--topic T] [--producer-id P] [--attempt A]
[--sequence S] [--expected-offset N] [--finish]`
: Appends one record per `--input` or `--input-file`, or per line of stdin.
  `--producer-id` with `--sequence` deduplicates a retry. `--finish` adds a
  `FINISH` record after the values.

`stream close --stream-id ID [--reason R]`
: Seals the stream. Records stay readable until retention passes.

`stream truncate --stream-id ID --to N`
: Moves the floor to `N`. Refused below an active consumer's floor.

`stream delete --stream-id ID [--force]`
: Deletes the stream and its records. Refused while a workflow consumes it,
  unless forced.

`--output json` prints one JSON object per record or list entry, and the
stream state proto for `describe`.

Authorization follows the server's declaration: `truncate` and `delete` need
the admin role on the namespace, `list`, `describe` and `read` the read role,
and the rest the write role.

## Notification channels

`temporal channel notify|describe|poll|listener add|listener remove` reach the
channel a stream notifies, or any other channel, by `--channel` (`-c`). Without
an owner the commands use the independent channel of that name. With
`--workflow-id W` or `--activity-id A`, and an optional `--run-id`, they use
the channel linked to that Workflow Execution or standalone Activity, sent as
the request's `execution` with the matching type. The two owner flags cannot
be combined, and `--run-id` alone is refused.

`channel describe` prints the kind and, for a linked channel, the owner line
`LinkedTo workflow W (run R)` or `LinkedTo activity A (run R)`, the run only
when the Service names one. The JSON output carries `linkedTo` as the Service
sends it.

## Refusals

A refusal the caller has to act on comes back from the server as a
`FailedPrecondition` whose message starts with a reason token. `create`,
`read` and `append` read the token and print what to do instead of the token,
with the server's own detail after it:

- `STREAM_PRODUCER_CONFLICT`: the producer already appended different content
  at this sequence; nothing was written.
- `STREAM_PRODUCER_STALE_SEQUENCE`: the sequence is below the producer's
  latest; nothing was written.
- `STREAM_CURSOR_BELOW_FLOOR`: the read starts below the floor; those records
  are gone.
- `STREAM_CLOSED`: the stream is closed; its records stay readable.
- `STREAM_POLICY_MISMATCH`: a stream with this id exists with a different
  lifecycle.

## Payload codec

The records' bodies and metadata go through the same remote codec as every
other payload the CLI shows or sends, configured with `--codec-endpoint`,
`--codec-auth` and `--codec-header`. The gRPC interceptor that applies the
codec walks only the public API's messages, so the stream commands apply it by
hand on the records and on a close reason.

## Examples

```sh
temporal server start-dev --port 7533

temporal stream create --address localhost:7533 --stream-id scores
temporal stream append --address localhost:7533 --stream-id scores \
    --producer-id game --sequence 1 --input '{"home": 1}' --input '{"home": 2}'
temporal stream read --address localhost:7533 --stream-id scores --follow
temporal stream describe --address localhost:7533 --stream-id scores

temporal stream read --address localhost:7533 \
    --workflow-id june-s1-abcdef12 --name scores
```
