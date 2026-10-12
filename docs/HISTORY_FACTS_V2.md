# History v2 source facts and bounded-source evidence

## Authority and profile

The 2026-10-12 continuation explicitly authorizes green milestone checkpoint pushes
to GaxCor/zapo codex/history-stream-v2 and GaxCor/whatsapp-crm-v2 codex/history-sync-v1,
without private real data/secrets. This candidate extends 082c93f from the accepted
7b805e8 lineage. It does not enable history, adopt an artifact or change the running CRM.

Opt-in `history.streamFacts: 'EXACT_V1'` requires explicit source resources plus
`streamLimits.facts.maxFactsPerChunk` and `maxFactChunkBytes`. No defaults. Omitting
the profile preserves the old event shape and message counts; v1 is unchanged.

BEGIN/SEAL identify the fact profile. FACT_CHUNK has independent contiguous chunk
sequence and fact ordinals. Existing message CHUNK sequence/count remains separate.
All callbacks share the original deadline and are awaited sequentially. SEAL carries
fact chunk/observation counts, not a whole-fact array. An unsealed capture is untrusted;
fact fulfillment alone never grants identity, canonical import, receipts or bot effects.
Consumers must prove coverage/digests of both lanes before fulfilling SEAL.

## Exact evidence, not heuristic resolution

The supported assertion source is only `HistorySync.phoneNumberToLidMappings`
(root15), with exact `pnJid`/`lidJid` fields from `PhoneNumberToLIDMapping`. This
decision is already encoded in the accepted source's history private-map consumer
and companion-host bootstrap encoder; no new request, tag, wire schema or crypto
behavior is invented. Upstream deobfuscated bundles remain unavailable here.

An IDENTITY_ASSERTION includes categorical EXPLICIT_PHONE_LID_PAIR, source/version,
physical source occurrence, exact addresses and a deterministic SHA256 evidence ID
over a version-qualified pair/source encoding. Its resolution is always UNRESOLVED.
The evidence ID is a pair fingerprint, not an authenticated source cursor, global
account identity, timestamp or permission to destructively merge bound conversations.

BEGIN/SEAL also preserve the source envelope's existing incoming-key classification:
SELF_ACCOUNT_INCOMING_KEY, OTHER_ACCOUNT_INCOMING_KEY or UNVERIFIED_SOURCE when the
parser is invoked without a real envelope. Source message ID/device are separate
facts; no new key/account/primary-device trust is inferred. The application must
reject/quarantine pair-resolution authority from another account or an unverified
parser-only source. A synthetic test peer is not the account's primary device;
its root15 pairs stay observations, never real identity acceptance.

Every duplicate and conflicting pair is emitted, not collapsed into a last-write map.
The consumer must retain occurrences and check all verified evidence and existing
binding/outbound/receipt constraints durably before resolving aliases. A conflicting
pair stays quarantined/auditable; this source contract does not select a winning pair.

Missing/noncanonical/device-qualified/repaired identities produce an explicit
IDENTITY_UNRESOLVED observation with a fixed reason and no relation. Duplicate
physical components likewise stay ambiguous. Unknown/wrong-wire/malformed mapping
fields reject the capture and cannot seal; inspected nested fields charge the original
source-record budget. Address admission is a conservative digit-only, non-device,
64-digit maximum user policy, not a newly asserted WhatsApp protocol limit.

Conversation pnJid/lidJid/accountLid/newJid/oldJid and inline-contact/name/avatar/body
metadata are not substituted for root15 evidence. No fuzzy/digit/name/content match
or inferred direction occurs. No address or callback payload belongs in diagnostics.

## Conversation facts and capabilities

CONVERSATION facts preserve original ordinal, DIRECT source address, optional source
name and exact source lastMsgTimestamp/conversationTimestamp values, including empty
conversations. They do not fabricate activity from ingestion time. Non-DIRECT facts
have counts/classification only, no detail. Repeated conversation identity fields are
ambiguous and fail closed in this profile. Old profiles retain their old behavior.

Unread count/marked-as-unread/history-transfer-end metadata is deliberately not
projected as operator unread or whole-account completion authority. Completeness
remains UNKNOWN. Actual message-window timestamps/anchors still come from message
observations; source facts do not add global recent-50 or pagination controls.

| Kind                                                      | Current capture status | Limits                                                                            |
| --------------------------------------------------------- | ---------------------- | --------------------------------------------------------------------------------- |
| Text, supported direct media/PTT, location, contact/vCard | SUPPORTED metadata     | Previous rich fixtures; no real retrieval/renderer claim                          |
| Quoted / forwarded                                        | PARTIAL                | Exact source ID/address/text-or-hint and flags/score; no quote direction inferred |
| GIF                                                       | PARTIAL                | Existing video gifPlayback/crypto-domain facts; not independent playback proof    |
| Reaction / revoke / polls                                 | PLACEHOLDER            | Known container hints only, no new event effects                                  |
| Ephemeral / view once                                     | PLACEHOLDER            | No implicit unwrap, media recovery or visibility semantics                        |
| System/security/call-related stubs                        | PLACEHOLDER            | No text/event fabrication from stub enum                                          |
| ROOT13 CallLogRecord                                      | EXCLUDED               | Explicit accepted exclusion remains                                               |
| Unknown / malformed                                       | UNKNOWN, fail closed   | No raw-protobuf fallback or trusted partial seal                                  |

CallLogRecord has fields named isIncoming/isVideo/callResult/startTime/duration and
per-participant results. The schema alone does not demonstrate timestamp/duration
units, which participant defines the direct conversation, result aggregation or
historical voice/video/missed-call authority. No accepted handler supplies those
semantics and bundles/real fixtures are absent. ROOT13 therefore remains EXCLUDED;
no silently widened call contract, duration invention or history-blocking call feature.

## Measured source working set

The new fact emitter retains one bounded fact chunk, not a root-map/conversation
collection. With privateMailbox DISABLED, root mappings are projected then released;
no historical body/deferred mailbox collection is built. The existing reader reuses
a bounded leaf buffer and consumes the decompression stream incrementally. Remaining
protocol-private tokens/salt/header state is separately admission-bounded and can
reject a token-heavy oversized source unit; no unbounded private spool is introduced.

One purpose-specific synthetic probe runs in separate processes, streams generated
protobuf through compression/decompression, consumes chunks without retaining them,
and yields between callbacks. Fixed limits:128 message rows/64KiB chunk,16 facts/8KiB,
4KiB leaf/header,8KiB private retention. All original repeated source occurrences
are counted; this is not canonical dedup, PostgreSQL staging or authenticated media
transfer. Probe transfer verification is synthetic; encrypted local flow tests supply
separate actual Noise/Signal evidence. Counts/timing are not real WhatsApp latency.

| Source observations | Inflated bytes | Baseline RSS bytes | Sampled peak RSS bytes | RSS growth bytes | Elapsed ms |
| ------------------: | -------------: | -----------------: | ---------------------: | ---------------: | ---------: |
|              10,000 |        990,034 |        118,329,344 |            138,092,544 |       19,763,200 |      6,128 |
|             100,000 |      9,900,035 |        108,556,288 |            128,290,816 |       19,734,528 |      1,007 |
|           1,000,000 |     99,000,035 |        119,762,944 |            139,161,600 |       19,398,656 |     22,572 |

The million-record case emits7,813 chunks, max128 rows, one conversation fact and
zero private mailbox writes. Process maxRSS reports139,165,696 bytes (sample interval
5ms plus every callback). All captures seal UNKNOWN completeness. The observer
timer continued, but is not an actual LIVE-message latency measurement. Runs shared
the host with build/typecheck work; absolute baseline/timing differences are disclosed,
not a controlled throughput SLA or proof for all token/media/source distributions.
RSS growth did not scale with the100x source count in these cases; no account-wide
constant-RSS, source exhaustion, production scale or application-progress claim.

## Tests and later gates

Eleven new source-fact tests cover exact/no/malformed/duplicate/conflicting evidence,
empty/non-DIRECT conversations, omitted profile, explicit budgets, callback rejection,
held/cancelled callback and late unknown mapping. All pass. Actual encrypted local
TEXT_V1, RICH_V2, RICH_FACTS and SELF_FACTS flows pass; the fact-profile flows receive LIVE while
both message and fact chunks are held, then observes one ordinary and one hist_sync
receipt only after SEAL fulfillment. No natural replay or real WhatsApp proof inferred.

SELF_FACTS uses actual local FakeWaServer pairing/reconnect with memory-only synthetic
material, never rendered or saved. It verifies SELF_ACCOUNT_INCOMING_KEY; the other
fact flow verifies OTHER_ACCOUNT_INCOMING_KEY. Parser-only tests retain UNVERIFIED_SOURCE.
Tests exposed an initially omitted worker forwarding argument and a fixture that had
not waited for post-pair reconnect; both were corrected, with assertions retained.

Final serial core:1,389 total,1,388 pass,zero fail,one optional undici skip. The
previous rich suite remains included. Core CJS/ESM/types build, full workspace
typecheck (ten tasks), lint, formatting and structure (54 directories) pass locally.
An earlier concurrency-two run during concurrent compilation failed the unchanged
ephemeral-cache 30ms TTL test; isolated ten-of-ten repeats and the final serial suite
passed. This timing-under-load failure is recorded, not patched or hidden.
These are local source gates, not CI or real acceptance. Distinct root version
1.9.1 is a candidate, not npm publication/tag or silent relabelling of the old artifact.
Reproducible packaging checks remain before adoption.
See [streaming/rich contract](./HISTORY_STREAM_V2.md).

Next: distinct reproducible candidate artifact and provenance, then the human-approved
CRM ADR/DB Change Spec implementation, durable capture/identity/recent-first scheduler/media/rendering
and all application gates. No real enablement until dedicated acceptance readiness;
stop for human actions then. Rollback preserves auth/data/backups and accepted v1.
