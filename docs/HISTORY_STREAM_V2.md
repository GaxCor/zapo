# History boundary v2: incremental capture candidate

## Authority and status

The GaxCor/zapo fork expansion was explicitly authorized on 2026-10-12 for the
CRM History Sync v1 milestone. This implementation checkpoint starts from the
previously accepted source `7b805e8d95fbdc27c89a1933e7173fe4099344b8`, not the fork's
unrelated master@1.9.0 line. It is an additive candidate, not an accepted/published
replacement package or real WhatsApp history acceptance. The root package version
still identifies the inherited source line; assign a distinct new version before
creating a candidate artifact. Do not relabel/repack it as the old CRM artifact.

No wire format, protobuf schema, stanza, query, cryptography or receipt builder is
changed. This slice reuses the accepted decoder and its exact dispositions. The
upstream bundles are not present in this checkout; any later new protocol behavior
must satisfy their verification gate before integration. This checkpoint makes no
new protocol-pagination, completeness, media or identity-authority claim.

## Supported opt-in surface

`history.onStream` is mutually exclusive with `history.onBatch`. It requires both
finite, explicit `boundaryLimits` and `streamLimits`. History-disabled clients cannot
configure a boundary. Clients that use v1 or no boundary keep their existing behavior.
The application can continue to default history off independently of the library's
inherited default. No configuration is added to the running CRM in this checkpoint.

`WaHistorySyncStreamEventV2` is exported from both public package entry points:

1. `BEGIN`: opaque delivery-scoped UUID and the same allowlisted notification facts
   as v1. Its callback must durably establish a provisional BUILDING capture.
2. `CHUNK`: one-based contiguous sequence, original contiguous observation ordinals,
   bounded normalized observations. Each callback is awaited before further parsing.
   Fulfillment means durable provisional staging, not canonical import or trust.
3. `SEAL`: verified EOF, integrity, complete family dispositions/conflict metadata,
   exact chunk/observation counts and completeness UNKNOWN. No full message array.
   Fulfillment must mean the complete capture was durably verified/accepted, with
   independent durable confirmation of ambiguous commits in the application.
4. `ABORT`: fixed reason `capture_failed`, no exception text or source values.
   Best-effort cleanup notice, never receipt authority. Abort-cleanup rejection
   cannot replace the original cause or authorize advancement.

The UUID is not a server cursor, source batch key or replay guarantee. On replay the
delivery UUID changes; the application needs exact durable manifest/chunk identities,
digests and provenance for idempotency. Version/profile changes must not reinterpret
old manifests or skip unprocessed observations. Recovery is not WhatsApp redelivery.

## Integrity, receipts and cancellation

The accepted parser may expose provisional normalized data before transfer integrity
is known. A late malformed/unknown field, bad MAC/hash, missing identity or source
classification failure prevents SEAL. Provisional observations must never create
canonical rows, source completeness, receipts or bot effects.

Only a fulfilled SEAL authorizes the ordinary delivery receipt. The existing private
settlement and `hist_sync` ordering then remain unchanged. BEGIN/CHUNK/ABORT never
complete that receipt gate. Callback success is not source completeness or a new
receipt authority for historical canonical messages.

Each callback shares one bounded worker job's original deadline. The worker admits
only sequential callbacks, not competing callbacks in the same job. Cancellation or
timeout with an unresolved callback faults history for this client instance, including
reconnect; late fulfillment cannot authorize a receipt. Library teardown detaches
the lifecycle and observes eventual promise settlement without retaining callback
payloads in a worker-owned completed-job collection.

ABORT is not invoked concurrently with a callback left unresolved by cancellation.
The consumer must cooperate with the supplied signal and recover/mark unsealed
BUILDING captures after restart. A lost process or late/ambiguous DB COMMIT cannot
be turned into success by an abort notice or by retrying an external side effect.

## Resource budgets and remaining limitation

`streamLimits.maxMessagesPerChunk` bounds observations per callback;
`maxChunkBytes` bounds the UTF-8 JSON observation array including brackets/commas,
with no silent truncation. An individually oversized projected observation rejects
the capture. `maxParkedMessages` independently limits source records that precede
Conversation.id; overflow rejects, never drops them. The inherited batch bytes,
source-field count and total admitted-message budgets still apply across all chunks.

The new observation emitter retains only one bounded chunk plus the current record,
not the whole batch array. This is **not yet a constant-space source implementation**:
private deferred message-write closures, header parts and batch-wide metadata remain
under the inherited finite batch admission ceilings. Their retention must be eliminated
or independently bounded/spilled in the next source slice. The consumer must not retain
all chunks in memory. No measured peak-memory/large-data gate is claimed here.

The executor uses the same JavaScript event loop, not a worker thread/process.
Asynchronous backpressure does not wait in the incoming LIVE path; synchronous
consumer CPU work can still block the event loop and must be bounded/yielding.

## Projection scope and privacy

This first slice uses the v1 TEXT/UNSUPPORTED observation projection. Duplicate source
occurrences and colliding stanza IDs remain distinct, with original ordinal/address
scope. It does not fetch media, infer aliases, create receipts or introduce a source
store/mailbox business-truth fallback. Non-DIRECT detail is still excluded. ROOT13
CallLogRecord remains excluded; unknown families remain fail closed.

Rich media/PTT, quoted/forwarded metadata, exact alias/conversation evidence and
protected retrieval descriptors are separate next slices with synthetic fixtures and
explicit semantic/security review. No AI, groups expansion or unread authority is added.
Do not log callback payloads: they contain private message text and source addresses.
Persist only through the protected capture path; operational outputs are fixed counts,
status/reason tokens and opaque internal IDs. Never expose media keys/locators or auth.

## Checkpoint tests and later gates

Local checkpoint gates: 180/180 focused history/client tests; 14 new core tests;
the new encrypted local fake-server cross-check passes. The full core run, with
concurrency 2 and flow-only names excluded, reports 1,348 tests: 1,347 passed,
zero failures and one optional-runtime skip (undici is absent for the existing
proxy-dispatcher test; no proxy test PASS is claimed). Structure (54 source directories),
core/examples/bench/all workspace typechecks, CJS/ESM/types build, lint, format and
diff checks pass. The fake-server test was also typechecked after addition.
These are local source-candidate gates, not new CI/PR or final milestone gates.

The new core suite covers contiguous/empty captures, count/UTF-8 byte backpressure,
oversize/gap rejection, cancellation and late settlement, actual parser staging and
verified-seal ordering, BEGIN/CHUNK/SEAL rejection, original-error preservation,
late unknown root, integrity rejection, explicit option validation and actual WaClient
receipt gating/worker fault behavior. Existing v1 history/client regressions also run.

The local fake-server cross-check uses the actual encrypted WaClient/Noise/Signal
flow with memory-only synthetic credentials. A held first chunk and a held seal do
not authorize history receipts; an independent synthetic LIVE contact arrives while
history is held. After SEAL fulfillment, exactly one ordinary delivery receipt and
one `hist_sync` receipt are actually observed by that local server. This is synthetic
source-flow evidence, not PostgreSQL durability or real WhatsApp acceptance.

Windows checkout CRLF caused the inherited test-only pinned-rule table extractor
to miss its LF delimiter. Local formatting normalization resolved it without changing
the pinned rules or weakening the schema assertion. No line-ending-only source/spec
changes are included in the checkpoint. Initial fake-flow test predicates were corrected
to the actual incoming `event.key.id` API and to distinguish ordinary and `hist_sync`
receipts; no product regression was hidden or source receipt assertion removed.

Before adoption: complete richer projection/resource work, distinct version and
immutable source artifact/provenance checks, compatibility/build/license/security
review, durable PostgreSQL consumer and schema gates, recovery/LIVE/resource gates,
dedicated real acceptance, exact-candidate CI and normal PR review/integration.
Do not enable history in the real unit merely because this checkpoint is green.

## Rollback

Revert the isolated source candidate/checkpoint if needed. No application dependency,
schema, auth/store/runtime or business data is changed by this candidate. Preserve
the accepted v1 artifact and running application; never erase paired auth to recover.
