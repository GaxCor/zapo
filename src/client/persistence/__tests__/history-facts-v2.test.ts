import assert from 'node:assert/strict'
import test from 'node:test'
import { gzipSync } from 'node:zlib'

import { projectHistoryIdentityAssertion } from '@client/persistence/history-facts-v2'
import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { HistorySyncStreamV2 } from '@client/persistence/HistorySyncStreamV2'
import type { WaClientOptions, WaHistorySyncStreamEventV2 } from '@client/types'
import { resolveWaClientBase } from '@client/WaClientFactory'
import { createNoopLogger } from '@infra/log/types'
import { proto } from '@proto'
import { concatBytes, TEXT_ENCODER } from '@util/bytes'

const PHONE = '15550000001@s.whatsapp.net'
const LID = '100000000000001@lid'
const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 65_536,
    maxBatchBytes: 65_536,
    maxMessagesPerBatch: 128,
    maxSourceRecordsPerBatch: 8_192,
    timeoutMs: 2_000
}
const STREAM = {
    maxMessagesPerChunk: 2,
    maxChunkBytes: 8_192,
    maxParkedMessages: 2,
    facts: { maxFactsPerChunk: 2, maxFactChunkBytes: 4_096 },
    resources: {
        maxRecordBytes: 8_192,
        maxConversationHeaderBytes: 8_192,
        maxParkedBytes: 8_192,
        maxRetainedPrivateBytes: 8_192,
        maxRetainedNotificationBytes: 8_192,
        maxThumbnailBytes: 1_024
    }
}
function project(pnJid?: string, lidJid?: string) {
    return projectHistoryIdentityAssertion(
        proto.PhoneNumberToLIDMapping.encode({ pnJid, lidJid }).finish(),
        1,
        () => undefined
    )
}
function config(events: WaHistorySyncStreamEventV2[]) {
    return {
        limits: LIMITS,
        streamLimits: STREAM,
        streamFacts: 'EXACT_V1' as const,
        privateMailbox: 'DISABLED' as const,
        signal: new AbortController().signal,
        onStream: async (event: WaHistorySyncStreamEventV2) => {
            events.push(event)
        }
    }
}
function fixture() {
    const calls: string[] = []
    const deps = {
        logger: createNoopLogger(),
        mediaTransfer: {} as never,
        writeBehind: {
            ownHistoryOperation: (start: () => Promise<void>) => start(),
            historyOperationsSettled: () => true,
            persistMessageAsync: async () => {
                calls.push('MESSAGE')
            },
            persistThreadAsync: async () => {
                calls.push('THREAD')
            },
            persistContactAsync: async () => {
                calls.push('CONTACT')
            }
        } as never,
        emitEvent: () => {
            calls.push('AGGREGATE')
        },
        onProcessed: async () => {
            calls.push('HIST_SYNC')
        }
    }
    return { deps, calls }
}

test('root-15 exact assertions retain categorical source and stable digest, never resolve/merge', () => {
    const first = project(PHONE, LID)
    const second = project(PHONE, LID)
    assert.equal(first.kind, 'IDENTITY_ASSERTION')
    assert.deepEqual(first, second)
    if (first.kind !== 'IDENTITY_ASSERTION') assert.fail('assertion expected')
    assert.equal(first.resolution, 'UNRESOLVED')
    assert.equal(first.source, 'HISTORY_SYNC_ROOT_15')
    assert.equal(first.evidenceVersion, 1)
    assert.match(first.evidenceId, /^[0-9a-f]{64}$/)
    assert.equal(first.phoneIdentity, PHONE)
    assert.equal(first.lidIdentity, LID)
    assert.equal('confidence' in first, false)
})

test('missing/invalid/device/approximate identities never create a pair', () => {
    for (const [phone, lid, reason] of [
        [undefined, LID, 'MISSING_PAIR'],
        [PHONE, undefined, 'MISSING_PAIR'],
        [PHONE.replace('@', ':1@'), LID, 'INVALID_PAIR'],
        [PHONE, LID.replace('@', ':1@'), 'INVALID_PAIR'],
        ['+15550000001@s.whatsapp.net', LID, 'INVALID_PAIR'],
        ['synthetic@s.whatsapp.net', LID, 'INVALID_PAIR'],
        [PHONE, PHONE, 'INVALID_PAIR'],
        [LID, LID, 'INVALID_PAIR']
    ]) {
        const fact = project(phone, lid)
        assert.equal(fact.kind, 'IDENTITY_UNRESOLVED')
        if (fact.kind !== 'IDENTITY_UNRESOLVED') assert.fail('unresolved expected')
        assert.equal(fact.reason, reason)
        assert.equal('phoneIdentity' in fact, false)
    }
})

test('duplicate components stay ambiguous and unknown/malformed mapping fields reject', () => {
    const first = proto.PhoneNumberToLIDMapping.encode({ pnJid: PHONE, lidJid: LID }).finish()
    const duplicate = concatBytes([
        first,
        proto.PhoneNumberToLIDMapping.encode({ pnJid: PHONE }).finish()
    ])
    assert.equal(
        projectHistoryIdentityAssertion(duplicate, 1, () => undefined).kind,
        'IDENTITY_UNRESOLVED'
    )
    for (const extra of [
        new Uint8Array([24, 1]),
        new Uint8Array([8, 1]),
        new Uint8Array([10, 255])
    ]) {
        assert.throws(() =>
            projectHistoryIdentityAssertion(concatBytes([first, extra]), 1, () => undefined)
        )
    }
    let charged = 0
    projectHistoryIdentityAssertion(first, 1, () => {
        charged++
    })
    assert.equal(charged, 2)
})

test('actual parser retains empty conversation facts, all duplicate/conflicting assertions and no unread/completeness authority', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = proto.HistorySync.encode({
        syncType: 3,
        progress: 100,
        conversations: [
            {
                id: PHONE,
                name: 'synthetic',
                unreadCount: 3,
                endOfHistoryTransfer: true,
                lastMsgTimestamp: 1_700_000_001,
                conversationTimestamp: 1_700_000_000,
                pnJid: PHONE,
                lidJid: LID
            },
            { id: 'synthetic@g.us', name: 'excluded' }
        ],
        phoneNumberToLidMappings: [
            { pnJid: PHONE, lidJid: LID },
            { pnJid: PHONE, lidJid: LID },
            { pnJid: PHONE, lidJid: '100000000000002@lid' }
        ]
    }).finish()
    await processHistorySyncNotification(
        f.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        config(events)
    )
    const facts = events.flatMap((event) =>
        event.type === 'FACT_CHUNK' ? [...event.sourceFacts] : []
    )
    assert.deepEqual(
        facts.map((fact) => fact.ordinal),
        [1, 2, 3, 4, 5]
    )
    assert.equal(facts.filter((fact) => fact.kind === 'IDENTITY_ASSERTION').length, 3)
    assert.equal(facts[0].kind, 'CONVERSATION')
    if (facts[0].kind !== 'CONVERSATION' || facts[1].kind !== 'CONVERSATION')
        assert.fail('conversation expected')
    assert.equal(facts[0].lastMsgTimestamp, '1700000001')
    assert.equal(facts[0].conversationTimestamp, '1700000000')
    assert.equal('unreadCount' in facts[0], false)
    assert.equal('endOfHistoryTransfer' in facts[0], false)
    assert.equal(facts[1].conversationAddress, undefined)
    assert.equal(facts[1].name, undefined)
    const seal = events.at(-1)
    assert.ok(seal?.type === 'SEAL')
    assert.equal(seal.factProfile, 'EXACT_V1')
    assert.equal(seal.sourceEnvelope?.origin, 'UNVERIFIED_SOURCE')
    assert.equal(seal.factObservationCount, 5)
    assert.equal(seal.factChunkCount, 3)
    assert.equal(seal.messageObservationCount, 0)
    assert.equal(seal.manifest.sourceHistoryCompleteness, 'UNKNOWN')
    assert.deepEqual(f.calls, ['AGGREGATE', 'HIST_SYNC'])
})

test('conversation/contact fields alone are not substituted for root-15 pair authority', async () => {
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = proto.HistorySync.encode({
        syncType: 3,
        conversations: [
            {
                id: PHONE,
                pnJid: PHONE,
                lidJid: LID,
                accountLid: LID,
                newJid: LID,
                oldJid: PHONE,
                name: 'synthetic'
            }
        ],
        inlineContacts: [{ pnJid: PHONE, lidJid: LID, fullName: 'synthetic' }]
    }).finish()
    await processHistorySyncNotification(
        fixture().deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        config(events)
    )
    const facts = events.flatMap((event) =>
        event.type === 'FACT_CHUNK' ? [...event.sourceFacts] : []
    )
    assert.deepEqual(
        facts.map((fact) => fact.kind),
        ['CONVERSATION']
    )
})

test('default v2 emits no fact events or profile, and retains old message counts', async () => {
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = proto.HistorySync.encode({
        syncType: 3,
        phoneNumberToLidMappings: [{ pnJid: PHONE, lidJid: LID }]
    }).finish()
    const defaults = config(events)
    await processHistorySyncNotification(
        fixture().deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        { ...defaults, streamFacts: undefined, streamLimits: { ...STREAM, facts: undefined } }
    )
    assert.deepEqual(
        events.map((event) => event.type),
        ['BEGIN', 'SEAL']
    )
    const seal = events.at(-1)
    assert.ok(seal?.type === 'SEAL')
    assert.equal(seal.factProfile, undefined)
    assert.equal(seal.factObservationCount, undefined)
})

test('fact byte budget is actual JSON including framing and refuses oversized facts', async () => {
    const fact = project(PHONE, LID)
    const bytes = TEXT_ENCODER.encode(JSON.stringify([{ ...fact, ordinal: 1 }])).byteLength
    for (const [limit, accepted] of [
        [bytes, true],
        [bytes - 1, false]
    ] as const) {
        const stream = new HistorySyncStreamV2(
            async () => undefined,
            { ...STREAM, facts: { ...STREAM.facts, maxFactChunkBytes: limit } },
            new AbortController().signal,
            'TEXT_V1',
            'EXACT_V1'
        )
        await stream.begin({ syncType: 3 })
        if (accepted) await stream.appendFact(fact)
        else await assert.rejects(stream.appendFact(fact), /fact-chunk byte limit/)
        await stream.abort()
    }
})

test('rejected fact callback cannot seal or advance', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = proto.HistorySync.encode({
        syncType: 3,
        phoneNumberToLidMappings: [{ pnJid: PHONE, lidJid: LID }]
    }).finish()
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
            {
                ...config(events),
                onStream: async (event) => {
                    events.push(event)
                    if (event.type === 'FACT_CHUNK') throw new Error('synthetic consumer failure')
                }
            }
        ),
        /synthetic consumer failure/
    )
    assert.equal(events.at(-1)?.type, 'ABORT')
    assert.equal(
        events.some((event) => event.type === 'SEAL'),
        false
    )
    assert.equal(f.calls.includes('HIST_SYNC'), false)
})

test('held fact callback stops parsing without allowing SEAL; cancellation never races ABORT', async () => {
    let release!: () => void
    let enter!: () => void
    const held = new Promise<void>((resolve) => {
        release = resolve
    })
    const entered = new Promise<void>((resolve) => {
        enter = resolve
    })
    const controller = new AbortController()
    const events: string[] = []
    const stream = new HistorySyncStreamV2(
        async (event) => {
            events.push(event.type)
            if (event.type === 'FACT_CHUNK') {
                enter()
                await held
            }
        },
        { ...STREAM, facts: { ...STREAM.facts, maxFactsPerChunk: 1 } },
        controller.signal,
        'TEXT_V1',
        'EXACT_V1'
    )
    await stream.begin({ syncType: 3 })
    const building = stream.appendFact(project(PHONE, LID))
    await entered
    assert.deepEqual(events, ['BEGIN', 'FACT_CHUNK'])
    controller.abort(new Error('synthetic cancellation'))
    await assert.rejects(building, /synthetic cancellation/)
    await stream.abort()
    release()
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.deepEqual(events, ['BEGIN', 'FACT_CHUNK'])
})

test('late unknown mapping cannot seal provisional conversation facts or acknowledge', async () => {
    const events: WaHistorySyncStreamEventV2[] = []
    const f = fixture()
    const mapping = concatBytes([
        proto.PhoneNumberToLIDMapping.encode({ pnJid: PHONE, lidJid: LID }).finish(),
        new Uint8Array([24, 1])
    ])
    const bytes = proto.HistorySync.encode({ syncType: 3, conversations: [{ id: PHONE }] })
        .uint32(122)
        .bytes(mapping)
        .finish()
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
            {
                ...config(events),
                streamLimits: { ...STREAM, facts: { ...STREAM.facts, maxFactsPerChunk: 1 } }
            }
        ),
        /unknown or malformed/
    )
    assert.equal(
        events.some((event) => event.type === 'FACT_CHUNK'),
        true
    )
    assert.equal(events.at(-1)?.type, 'ABORT')
    assert.equal(
        events.some((event) => event.type === 'SEAL'),
        false
    )
    assert.equal(f.calls.includes('HIST_SYNC'), false)
})

test('fact options are explicitly bounded and cannot be applied to v1/no-boundary clients', () => {
    const valid = {
        onStream: async () => undefined,
        boundaryLimits: LIMITS,
        streamLimits: STREAM,
        streamFacts: 'EXACT_V1'
    }
    const resolve = (history: unknown) =>
        resolveWaClientBase(
            {
                sessionId: 'synthetic',
                store: { session: () => ({}) },
                history
            } as unknown as WaClientOptions,
            createNoopLogger()
        )
    assert.doesNotThrow(() => resolve(valid))
    assert.throws(() => resolve({ streamFacts: 'EXACT_V1' }), /require history.onStream/)
    assert.throws(() => resolve({ ...valid, streamFacts: 'UNKNOWN' }), /unsupported/)
    assert.throws(
        () => resolve({ ...valid, streamLimits: { ...STREAM, facts: undefined } }),
        /explicit/
    )
    assert.throws(
        () => resolve({ ...valid, streamLimits: { ...STREAM, resources: undefined } }),
        /explicit/
    )
    assert.throws(() => resolve({ ...valid, streamFacts: undefined }), /require streamFacts/)
    for (const key of ['maxFactsPerChunk', 'maxFactChunkBytes'])
        for (const value of [0, -1, NaN, Infinity, 1.5])
            assert.throws(
                () =>
                    resolve({
                        ...valid,
                        streamLimits: { ...STREAM, facts: { ...STREAM.facts, [key]: value } }
                    }),
                /positive safe/
            )
})
