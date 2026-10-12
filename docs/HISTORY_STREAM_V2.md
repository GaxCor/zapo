# History boundary v2: incremental capture candidate

The later opt-in [source-fact contract](./HISTORY_FACTS_V2.md) adds exact root15
assertions, conversation facts and measured bounded-source evidence. Old profiles
retain the event/count contract recorded here.

## Authority and status

The GaxCor/zapo fork expansion was explicitly authorized on 2026-10-12 for the
CRM History Sync v1 milestone. This implementation checkpoint starts from the
previously accepted source `7b805e8d95fbdc27c89a1933e7173fe4099344b8`, not the fork's
unrelated master@1.9.0 line. It is an additive candidate, not an accepted/published
replacement package or real WhatsApp history acceptance. The first checkpoints
retained the inherited version; the source-facts candidate now has distinct version
1.9.1 for reproducible packaging. Do not relabel/repack it as the old CRM artifact.

No wire format, protobuf schema, stanza, query, cryptography or receipt builder is
changed. This slice reuses the accepted decoder and its exact dispositions. The
upstream bundles are not present in this checkout; any later new protocol behavior
must satisfy their verification gate before integration. This checkpoint makes no
new protocol-pagination, completeness or identity-authority claim. Rich metadata
projection below is not real media availability, retrieval or acceptance evidence.

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
`maxChunkBytes` bounds the UTF-8 JSON metadata array including brackets/commas,
plus a conservative charge for each protected media handle in RICH_V2,
with no silent truncation. An individually oversized projected observation rejects
the capture. `maxParkedMessages` independently limits source records that precede
Conversation.id; overflow rejects, never drops them. The inherited batch bytes,
source-field count and total admitted-message budgets still apply across all chunks.

`streamLimits.resources` is required for RICH_V2 and optional for TEXT_V1. All six
ceilings are explicit positive safe integers; none has a hidden default:

- `maxRecordBytes`: inspected record/non-descended field ceiling, additionally capped
  by inherited record/batch limits. A large conversation is descended incrementally.
- `maxConversationHeaderBytes`: retained header wire bytes plus per-entry charges.
- `maxParkedBytes`: detached records before Conversation.id plus per-entry charges;
  independent of `maxParkedMessages`. Boundary mode does not also build a decoded
  private-message parking collection for the same source records.
- `maxRetainedPrivateBytes`: conservative source-unit retained payload/backing-buffer,
  string and entry charges. It includes deferred body writes in PRESERVE mode, root
  contact/map/name collections, trusted-contact tokens and salt. Shared backing buffers
  may be charged more than once; this is conservative admission, not measured RSS.
- `maxRetainedNotificationBytes`: independent worker admission ceiling for retained
  notification backing allocations, including nested bytes, before any capture starts.
- `maxThumbnailBytes`: admitted JPEG-signature thumbnail source bytes. Oversize or
  unsupported thumbnails are explicitly omitted, without dropping message metadata.

Every overflow rejects the capture, cannot seal and cannot advance source receipts.
The opt-in `privateMailbox: 'DISABLED'` avoids historical message/body re-encoding,
deferred body closures and private mailbox thread/contact/name/map caches/writes.
It does **not** disable trusted-contact tokens, NCT salt, ephemeral chat metadata or
other protocol-private state. Omission/PRESERVE retains the previous private archive.
Mailbox disabling is not a business-truth fallback or an identity-evidence import.
Small retained descriptors/header/salt bytes are detached from source allocations.

The observation emitter retains one bounded chunk plus the current record, not the
whole batch array. Remaining protocol-private source-unit collections are independently
budgeted; overflow rejects rather than inventing a spill protocol. This is **not a
measured constant-space/RSS guarantee**. The consumer must stage chunks durably and
not retain them all. Per-job ceilings multiply by configured concurrency; source-unit
private state remains finite, not persisted application checkpoints. No progressive
large-data/peak-memory acceptance gate is claimed here.

The executor uses the same JavaScript event loop, not a worker thread/process.
Asynchronous backpressure does not wait in the incoming LIVE path; synchronous
consumer CPU work can still block the event loop and must be bounded/yielding.

## Projection scope and privacy

Omitting `streamContent` or selecting TEXT_V1 keeps the v1 TEXT/UNSUPPORTED projection.
The opt-in RICH_V2 profile is marked on BEGIN/SEAL; it must not reinterpret older stored
captures. Duplicate source
occurrences and colliding stanza IDs remain distinct, with original ordinal/address
scope. It does not fetch media, infer aliases, create receipts or introduce a source
store/mailbox business-truth fallback. Non-DIRECT detail is still excluded. ROOT13
CallLogRecord remains excluded; unknown families remain fail closed.

RICH_V2 projects already structurally validated DIRECT records, without new schema,
wire requests, unwrap behavior or receipt authority:

| Source fact                                                                                    | Candidate projection                                                | Limits                                                                                         |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Text / extended text                                                                           | TEXT, original safe-as-text value                                   | No HTML execution or source mutation                                                           |
| Image, video/GIF, audio, PTT, document, sticker                                                | Typed metadata, caption, dimensions/duration/flags when present     | Metadata only, no retrieval or playback proof                                                  |
| File length                                                                                    | Exact unsigned uint64 decimal string                                | Never rounded into transfer-helper numeric length                                              |
| Thumbnail                                                                                      | Bounded JPEG-signature bytes as base64, or explicit omission        | Signature is not decoder validity; renderer must still fail safely                             |
| Descriptor                                                                                     | Protected separate handle                                           | Relative locator, supported crypto type, exact 32-byte key/hashes, conservative MIME allowlist |
| Location                                                                                       | Finite ranged coordinates and optional source name/address          | No geocoding/live-location inference                                                           |
| Contact/vCard                                                                                  | Source card display name/vCard                                      | No parsing into aliases or identity merges                                                     |
| Forwarded / quoted                                                                             | Only explicit source flags/score and exact quote ID/address/preview | No direction inferred; unsupported quote is a hint only                                        |
| Wrappers, view-once (including direct flag), reactions, revoked/protocol and other known kinds | UNSUPPORTED placeholder                                             | No implicit unwrap, new semantics or retrieval descriptor                                      |

Conflicting direct/quoted content fields reject, rather than choose an arbitrary
payload. Unknown/malformed protocol fields retain the accepted fail-closed gate.
Unavailable/invalid descriptors preserve message metadata with a fixed UNAVAILABLE
reason; DESCRIPTOR_AVAILABLE is not a CDN availability/expiry or integrity claim.
Signed/absolute URLs are not a fallback; MIME/filename guards do not replace actual
fetch response/size/cache validation in the later bounded media pipeline.

`WaHistorySyncMediaSourceV2` holds detached descriptor bytes in a WeakMap with no
enumerable secrets. JSON serialization throws; Node inspection is masked. A consumer
must remove `mediaSource` from public metadata JSON, explicitly export it only into
protected encryption-at-rest persistence, and make metadata+descriptor durable as
one provisional capture operation. `getTransferInput()` returns sensitive detached
copies for protected persistence/the existing transfer API. It performs no request;
callers must separately impose actual byte/MIME/response limits. Never log that export.

Exact alias/conversation evidence remains a separate next source slice. No AI, groups
expansion, unread authority, call-log inclusion or CRM media renderer is added.
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

That paragraph records the first streaming checkpoint, not the newer rich slice.
The richer follow-up adds 30 synthetic core tests and extends the encrypted local
cross-check to TEXT_V1 and RICH_V2. Both local flows receive LIVE while a CHUNK is
held, withhold history receipts through a held SEAL, then observe exactly one
ordinary and one hist_sync receipt. These are synthetic source-flow observations,
not real WhatsApp, PostgreSQL durability, replay or media-fetch evidence.

Final rich-slice core run: **1,378 total, 1,377 pass, zero failures, one skip**,
concurrency 2, flow-only names excluded. The optional proxy-dispatcher test was
rerun independently and again skipped because undici is absent; no proxy PASS is
claimed. Final new core + encrypted cross-check run is **32/32** (30 + 2).
The 187-test focused regression run passed before the last two parser cases were
added; those two are included in the final full/new-test runs. Structure remains
54 source directories. CJS/ESM/types build, core/examples/bench/workspace typechecks,
full lint plus last-change lint, formatting and whitespace gates pass locally.

The new tests exercise each metadata kind, quoted/forwarded facts, disabled-mailbox
protocol-private hooks, default/v1 compatibility, protected-handle serialization and
detached buffers, relative-locator/MIME/filename/uint64/thumbnail guards, chunk handle
charges, record/header/parked/private/notification retention rejection, reversed field
order and actual-parser ambiguity/exclusions. An initial fixture incorrectly called
a non-existent generated fromObject API; the fixture was corrected to encode/decode.
Its uint64 case then exposed that the existing transfer helper rejects unsafe numeric
length conversion. The rich projector now keeps the exact decimal separately and
passes descriptor-only fields to that unchanged helper. No assertion was weakened,
no transfer/protocol decision was patched and no private source data was used.

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

The source-facts follow-up records the completed rich/resource source gates.
Before adoption: immutable source artifact/provenance checks, compatibility/build/license/security
review, durable PostgreSQL consumer and schema gates, recovery/LIVE/resource gates,
dedicated real acceptance, exact-candidate CI and normal PR review/integration.
Do not enable history in the real unit merely because this checkpoint is green.

## Rollback

Revert the isolated source candidate/checkpoint if needed. No application dependency,
schema, auth/store/runtime or business data is changed by this candidate. Preserve
the accepted v1 artifact and running application; never erase paired auth to recover.
