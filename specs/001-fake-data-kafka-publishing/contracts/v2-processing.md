# v2 detailed processing and audit contract

This contract refines final-topic-v2.md for FR-032–037. Implementation is pending.

## Run identity and audit boundary

New runs require explicit --new-run and a target. Resume requires --run-id; ordinary
restart never creates a run. A unique filtered SQL constraint permits one Active run.
Run counter increment, entity updates and Outbox inserts share one transaction. The
last batch closes the run in that transaction; legacy interrupted closure is reconciled
by checking its persisted count against its manifest before generating anything.
SequenceNo remains globally increasing; target and count are scoped to RunId. A new
run starts a fresh active-entity pool; entities do not span runs in v2.

Provision a fresh pipeline before any generation. Register per-partition final-topic
start offsets (zero for fresh topics), cluster/topic UUIDs and pipeline identity.
Every run in that pipeline inherits these audit starts. Audit the entire retained
pipeline history from these starts to captured ends: select RunId in counts, validate
other registered runs against their immutable manifests and report them separately.
Unknown runs or malformed records fail the audit. Low watermark above a registered
start means incomplete history; never silently replace start with current earliest.
Capture ends only after selected run is closed and ingress/final processing is drained;
persist the boundaries with the report. No claims about writes after captured ends.

## Immutable catalog and routing validation

The finalizer reads authoritative Outbox identity/content metadata from SQL before
forwarding any first-seen event. Use batched read-only lookups and cache only immutable
positive results. Validate EventId's RunId, key, sequence, type, canonical content and
explicit source partition against SQL; reject unknown or mismatched copies before
forwarding. This prevents the same identity with a changed key from escaping into a
different partition's local dedup store. Do not repartition input by EventId.
SQL unavailability pauses/retries processing without offset advancement; it is not
an atomic SQL/Kafka transaction. No mutable SQL delivery flags influence dedup decisions.
Outbox identity/content columns are write-once through restricted roles/procedures;
only delivery metadata is mutable. Do not remove catalog rows during replay horizon.

## Pipeline binding and reset guard

SQL PipelineRegistration binds unique (ClusterId, FinalTopicId) to PipelineId,
ApplicationId, IngressTopicId, internal topic IDs, six partitions, routing/comparison
versions and lifecycle Provisioning -> Ready -> Suspended -> Retired. Provisioning
requires empty fresh topics and no business input. Precreate/register changelog topics
with explicit store names; transition Ready only after empty-state bootstrap validation.
A crash during provisioning resumes that same registration, never creates a new binding.

Runtime identities cannot create/delete topics, delete records, reset group offsets or
modify the registry. Startup validates topic IDs (not just names), settings, registration
and restore completion before business forwarding. Missing/recreated changelog, changed
application.id or routing version causes failure; it is never auto-created for Ready
pipelines. Admin destructive resets are unsupported and require suspension and a fresh
pipeline. Poll topology identity/config changes and stop on mismatch; permanent storage
loss and malicious administrative interference are outside durable-storage guarantees.

For additional restore validation, each task before its first forward after assignment
captures its final partition end, scans committed output to that boundary and checks
that every emitted identity/hash exists identically in its restored store. A final record
missing from state suspends the task, never reconstructs state silently. Other tasks may
continue on their own partitions. Rebalance invalidates the validation and reruns it.
This detects emptied state even if a topic name/id survived. Full scans can exceed the
18-second target: measure it, do not waive that target or claim this design already meets it.

## Canonical content version decimal-json-v1

Parse JSON with exact decimal number tokens, never binary floating point. Reject duplicate
object names, invalid Unicode/unpaired surrogates, NaN/Infinity, depth >64, number token
>1024 characters, absolute exponent >10000 and payload >1 MiB. These limits are validated
before event creation and again on consumption. Strings are decoded scalar values with
no Unicode normalization. Sort object keys by Unicode scalar value; preserve array order.

Canonical bytes are a typed, length-prefixed UTF-8 grammar: null='N'; boolean='T'/'F';
string='S'+decimalByteLength+':'+rawUTF8; number='D'+length+':'+normalizedDecimal;
array='A'+elementCount+':'+concatenated canonical values; object='O'+memberCount+':'
+concatenated canonical string keys and values sorted by key. Counts have no leading zeros.
A number is sign, nonzero coefficient digits without leading/trailing zeroes, 'e', signed
base-10 exponent without '+' or leading zeroes. Example 1,1.0,1e0 => 1e0; 100=>1e2;
-0=>0; all zeros=>0. SHA-256 of canonical bytes is the payload fingerprint. Metadata
fields are compared individually, not hidden in the payload digest. This grammar replaces
the earlier compact-JSON/ordinal-sort wording, which did not define precision equivalence.
Fixtures must include astral Unicode keys, escapes, decimals >2^53, negative zero, duplicate
names and invalid limits. Store version alongside hash; refuse mismatched versions.

## CLI and settings (planned interfaces)

Generator: --new-run --target-events N --pipeline-id UUID, or --run-id UUID to resume.
Verifier: --verify --run-id UUID; exits 0 only on strict success, 2 on validation failure,
3 on incomplete/dependency failure. Existing --verify without RunId fails with guidance
in strict mode. Credentials remain environment/local files, never command arguments.
Finalizer: --config PATH --check-only or --config PATH. Configuration includes application.id,
pipeline.id, bootstrap servers, input/output names, SQL catalog connection, state.dir,
processing.guarantee=exactly_once_v2 and producer compression=lz4. All fixed invariants are
validated; state.dir differs per instance. Broker roles restrict final writes to finalizer.

## Recovery and baseline

Use deterministic barriers and an external read_committed observer ready before crash.
Measure from successful termination acknowledgement on one monotonic test-controller
clock to observation of the first new committed final record per affected partition.
All affected partitions have backlog and an eligible survivor. Report maximum and timing
uncertainty; upper bound >18 seconds fails. Verify all identities afterward.

Baseline key includes contract v2, environment, partition count, manifest hash version,
processor build/topology version, batch settings and replication profile. Measure final
output throughput from observer data, not SQL ingress timestamps. Historical v1 baselines
remain separate and are never promoted to v2. Count duplicate ingress separately.

## U7 assignment-scoped recovery protocol — 2026-11-23

This section refines the restore audit above. Fencing is owned by Kafka Streams EOS
and its consumer-group protocol, not by a second application-created producer using
Streams transactional IDs. A RUNNING notification or a single restore callback alone
is not a proof that a consistent audit boundary is available.

Each task carries an in-memory AssignmentToken (process incarnation, task ID, local
assignment counter). It is a cancellation/staleness token, not Kafka fencing authority.
The recovery state sequence is BLOCKED -> OWNERSHIP_READY -> RESTORED -> BOUNDED ->
AUDITING -> VALIDATED -> PROCESSING. No business forward, dedup mutation or input
position advancement is allowed through the application gate before VALIDATED.

1. On acquisition/reinitialization create a new token, close the forwarding gate and
   discard prior audit results. Validate registry/topic IDs and let Streams establish
   active ownership and initialize its EOS transactional processing. Old-owner work
   must be resolved through the supported Streams group/transaction protocol.
2. Let Streams complete restoration of every store of this task to committed state.
   A standby's acceptable lag or a callback for just one store is insufficient.
   Keep the task's state stable (no business mutations) during the audit.
3. Only after ownership and restoration readiness, capture the final partition high
   watermark H and topic UUID. Wait until read_committed can reach H (last stable
   offset at least H), so earlier open transactions are committed or aborted. Capture
   the committed changelog boundary C after that wait; ensure the restored state is
   caught up to C before comparing. A read-only evidence consumer does not replace
   Streams restoration or mutate the store.
4. Scan committed business output from the registered audit start up to H, compare
   it to the task's committed restored store, and retain token/topic IDs/H/C in the
   result. After scanning, confirm ownership token and topic IDs are unchanged and
   the final high watermark is still H. If output advanced, invalidate the result
   and repeat boundary/restore validation, never silently extend a completed scan.
5. Immediately before opening the gate, recheck token and active ownership in the
   serialized task execution context. Only this current assignment's successful audit
   permits processing. Store references/results cannot outlive their assignment.

On revoke, task loss, producer fencing, shutdown or any intervening rebalance,
close the gate first, cancel reads and discard H/C/results. Late asynchronous results
cannot reopen the gate. Streams owns abort/cleanup and re-restoration; application
code must not independently commit/abort its producer. Reacquisition starts from
BLOCKED with a fresh token, even on the same process. No successful audit is cached
across assignments. Unresolved transactions, incomplete scan, timeout or lost ownership
leave the gate closed and report incomplete recovery; the 18-second gate is unchanged.

Integration feasibility gate: T096 must identify supported lifecycle hooks and prove
these happens-before relations on the pinned Streams version, including the task-thread
handoff for asynchronous evidence reads. A generic StateRestoreListener alone is not
sufficient. If supported APIs cannot enforce the protocol, stop T097 and return to
speckit-plan; do not bypass the audit, patch private internals or claim U7 runtime proof.

Required deterministic tests: hold old-owner commit open across takeover (commit and
abort variants); delay final-record visibility; rebalance at every state including just
before gate-open; deliver an old audit completion after reassignment; verify gate stays
closed and stale producer cannot make a committed final record after ownership loss.
This protocol defines ordering only. U8 drain criteria, U9 independent error handling
and C1 reverse state/output comparison remain separate pending corrections.

## U8 closed verification barrier — 2026-11-23

This section defines "drained" above. SQL pending=0, consumer local position, cached
lag=0 or an idle delay alone cannot establish final completion.

1. Require selected RunId Closed and freeze its manifest. A nonzero selected-run
   dead-letter count is strict validation failure (exit 2). Wait for every selected
   event to have an ingress acknowledgement; missing acknowledgement never counts
   as success even if an earlier ambiguous transaction actually committed.
2. Acquire a persisted pipeline-wide verification barrier with an epoch. It prevents
   new-run admission and pauses all ingress lanes between batches for this pipeline.
   Existing active generation may finish its current atomic batch and pause; it is
   not silently closed. Each lane acknowledges the current epoch only after its
   current Kafka transaction and SQL mark have completed or resolved. Lease takeover
   and restarted instances must observe the barrier before opening a new batch.
   Finalizer remains running. This is an administrative verification pause, not a
   change to normal independent partition processing.
3. A crashed/unresponsive ingress lane is not presumed quiet from timeout alone.
   Require completion acknowledgement or a replacement owner to fence/recover it and
   acknowledge the epoch. The existing ingress lease/fencing protocol is used; verifier
   does not create a competing producer. Hold the barrier throughout final capture.
4. Capture ingress topic UUID and high watermark H[p] for all six partitions. A
   separate read_committed observer scans to each captured end (including evidence
   of skipped aborted/control offsets). If open transactions prevent reaching H[p],
   continue waiting within the deadline. Record B[p] = last committed business-record
   offset below H[p] + 1. For partitions with no business records, no group-offset
   predicate is needed. Do not require committed group offset >= H[p]: H[p] may
   include transaction markers that do not correspond to processed business records.
5. Poll the REGISTERED finalizer consumer group's broker-committed next offsets,
   not its fetched/local positions. For each nonempty partition require offset >= B[p].
   Under the required EOS-v2 state/output/offset transaction this proves input through
   the cut has committed processing effects, including deduplicated input records.
   Missing offset, stopped task, wrong group ID or unresolved transaction keeps the
   barrier incomplete. Check the same pipeline/application/topic registration throughout.
6. Capture final topic UUID and high watermark F[p]. Consume read_committed through
   all F[p] and perform strict set/content checks. Recheck ingress high watermarks are
   still H[p], lane acknowledgements have the same barrier epoch, and registrations
   and manifest digest are unchanged. If any change, invalidate this snapshot; never
   silently extend a completed scan or publish a success result from it.
7. Persist snapshot evidence and outcome, then release the barrier epoch. Results
   certify the captured prefix, not subsequent writes. On failure/cancellation release
   through the owner coordinator in finally; after coordinator crash the barrier stays
   closed until an explicit recover/release operation, never expires into silent resume.

Use one monotonic deadline for the entire barrier/capture/drain/verification attempt:
Reporting.VerificationTimeoutSeconds defaults to 600, configurable positive integer.
Polling/API retries share the remaining budget; calls cannot wait past cancellation.
Timeout, missing dependency, unavailable retained history or barrier invalidation gives
VerificationComplete=false, exit 3 and no baseline. Persist which stage/partition
blocked and the last observed offsets. Detected content errors yield exit 2 and no
strict pass. A new attempt uses a new VerificationId/epoch; preserve failed evidence.
This timeout is separate from the unchanged 18-second process-recovery requirement.

Integration tests must keep finalizer behind after SQL pending becomes zero, delay
an open transaction, include aborted/control-only tails, kill a lane during barrier
entry, exercise a second run on the same pipeline, and cancel/timeout mid-drain.
Only complete broker-committed input processing plus strict final audit may pass.
Seeded US5 fixtures may provide a test finalizer that commits input offsets and output
atomically; an invented lag counter is not sufficient integration evidence.

## U9 partition-error isolation gate — 2026-11-23

Status: OPEN architecture blocker, not a completed isolation design. Prior statements
that a task can simply suspend while other tasks progress are requirements, not a
verified Kafka Streams 4.1.2 mechanism. Increasing num.stream.threads alone is not
proof of one partition per failure/transaction domain after reassignment.

Required policy: transient failure retains the failing event and blocks later final
output for that partition; permanent validation/conflict failure fails that run and
keeps its partition blocked. No CONTINUE/skip, manual offset advancement, separate
retry-topic reordering or mutation of Streams-owned transactions is permitted.
Kafka runtime owns abort and committed-state restoration. Fencing/task loss invalidates
U7 tokens. Global SQL/broker outage may affect all partitions and is reported separately
from a partition-local injected failure. U8 verification remains incomplete while a
required partition cannot progress; deadline does not authorize dropping its event.

The documented processing exception handler supplies FAIL or CONTINUE, not a general
partition-local retry. Production-handler RETRY is limited to eligible producer errors
and does not implement SQL/catalog/restore-audit retry. A blocking callback/retry on a
shared thread can block unrelated tasks; a thread-wide abort may affect their batch.
Therefore no handler-only patch can be assumed to satisfy the independence requirement.

Before finalizer implementation, a bounded feasibility prototype must verify a supported
assignment/execution isolation model: ownership remains valid during local retry,
heartbeats continue, the failed offset does not advance, later records do not overtake,
and healthy partitions keep committing through repeated failures and rebalance/takeover.
Capture task-to-thread/process mapping and transaction effects, not only output totals.
If custom TaskAssignor is considered, prove its supported assignment granularity and
capacity behavior on the pinned version; do not equate process assignment with thread
isolation. Any proposal needing additional instances, topics or topology changes is
returned for explicit architecture approval before adoption.

U9 remains unresolved until this prototype produces a concrete supported policy and
its evidence is incorporated through speckit-plan/tasks/analyze. Do not claim the
previous design constitution gate passed for partition independence. U9 implementation
is blocked; unrelated documentation corrections still may proceed with approval.
