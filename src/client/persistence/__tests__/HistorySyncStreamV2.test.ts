import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import { Readable } from 'node:stream'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { gzipSync } from 'node:zlib'

import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { HistorySyncBoundaryWorker } from '@client/persistence/HistorySyncBoundaryWorker'
import { HistorySyncStreamV2 } from '@client/persistence/HistorySyncStreamV2'
import type {
    WaClientOptions,
    WaHistorySyncBatchV1,
    WaHistorySyncMessageObservation,
    WaHistorySyncStreamEventV2,
    WaHistorySyncStreamHandler
} from '@client/types'
import { WaClient } from '@client/WaClient'
import { resolveWaClientBase } from '@client/WaClientFactory'
import { createNoopLogger } from '@infra/log/types'
import {
    handleIncomingMessageAck,
    type WaDeferredHistoryReceipt
} from '@message/primitives/incoming'
import { proto } from '@proto'
import { TEXT_ENCODER } from '@util/bytes'

const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 1_000_000,
    maxBatchBytes: 1_000_000,
    maxMessagesPerBatch: 128,
    maxSourceRecordsPerBatch: 8_192,
    timeoutMs: 2_000
}
const STREAM_LIMITS = { maxMessagesPerChunk: 2, maxChunkBytes: 4_096, maxParkedMessages: 2 }
const MANIFEST: Omit<WaHistorySyncBatchV1, 'version' | 'messageObservations'> = {
    sourceHistoryCompleteness: 'UNKNOWN',
    notification: { syncType: 3 },
    decoded: { syncType: 3 },
    parseReachedEof: true,
    payloadIntegrityVerified: true,
    sourceFamilyDispositions: [],
    metadataConflicts: []
}
const ADDRESS = 'synthetic@s.whatsapp.net'

function deferred() {
    let resolve!: () => void
    let reject!: (error: Error) => void
    const promise = new Promise<void>((yes, no) => {
        resolve = yes
        reject = no
    })
    return { promise, resolve, reject }
}

function observation(ordinal: number, text = 'synthetic'): WaHistorySyncMessageObservation {
    return {
        ordinal,
        conversationOrdinal: 1,
        chatKind: 'DIRECT',
        disposition: 'IMPORT_DIRECT',
        externalMessageId: `synthetic-${ordinal}`,
        conversationAddress: ADDRESS,
        fromMe: false,
        content: { kind: 'TEXT', text }
    }
}

function payload(count = 5): Uint8Array {
    return proto.HistorySync.encode({
        syncType: 3,
        progress: 100,
        conversations: [
            {
                id: ADDRESS,
                messages: Array.from({ length: count }, (_, index) => ({
                    message: {
                        key: { remoteJid: ADDRESS, fromMe: false, id: `synthetic-${index}` },
                        messageTimestamp: 1_700_000_000 + index,
                        message: { conversation: 'synthetic' }
                    }
                }))
            }
        ]
    }).finish()
}

function fixture() {
    const order: string[] = []
    const deps = {
        logger: createNoopLogger(),
        mediaTransfer: {} as never,
        writeBehind: {
            ownHistoryOperation: (start: () => Promise<void>) => start(),
            historyOperationsSettled: () => true,
            persistMessageAsync: async () => {
                order.push('PRIVATE_MESSAGE')
            },
            persistThreadAsync: async () => undefined,
            persistContactAsync: async () => undefined
        } as never,
        emitEvent: () => {
            order.push('AGGREGATE')
        },
        onProcessed: async () => {
            order.push('HIST_SYNC')
        }
    }
    return { deps, order }
}

function options(handler: WaHistorySyncStreamHandler) {
    return {
        signal: new AbortController().signal,
        limits: LIMITS,
        onStream: handler,
        streamLimits: STREAM_LIMITS
    }
}

test('v2 splits contiguous observations without a full batch at SEAL, including empty capture', async () => {
    for (const count of [0, 1, 5]) {
        const events: WaHistorySyncStreamEventV2[] = []
        const controller = new AbortController()
        const stream = new HistorySyncStreamV2(
            async (event) => {
                events.push(event)
            },
            STREAM_LIMITS,
            controller.signal
        )
        await stream.begin(MANIFEST.notification)
        for (let index = 1; index <= count; index++) await stream.append(observation(index))
        await stream.seal(MANIFEST)
        const chunks = events.filter((event) => event.type === 'CHUNK')
        assert.deepEqual(
            chunks.map((chunk) => chunk.sequence),
            chunks.map((_, index) => index + 1)
        )
        assert.deepEqual(
            chunks.flatMap((chunk) => chunk.messageObservations.map((row) => row.ordinal)),
            Array.from({ length: count }, (_, index) => index + 1)
        )
        assert.ok(chunks.every((chunk) => chunk.messageObservations.length <= 2))
        assert.equal(new Set(events.map((event) => event.captureId)).size, 1)
        const seal = events.at(-1)
        assert.ok(seal?.type === 'SEAL')
        assert.equal(seal.messageObservationCount, count)
        assert.equal(seal.chunkCount, chunks.length)
        assert.equal('messageObservations' in seal.manifest, false)
        assert.equal(seal.manifest.sourceHistoryCompleteness, 'UNKNOWN')
        assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
        await assert.rejects(stream.append(observation(count + 1)), /not building/)
        await assert.rejects(stream.seal(MANIFEST), /not building/)
        await stream.abort()
        assert.equal(events.at(-1)?.type, 'SEAL')
    }
})

test('v2 enforces actual UTF-8 JSON array budget, including multibyte and escaped text', async () => {
    const rows = [observation(1, '😀\n"'), observation(2, '😀\n"')]
    const bytes = TEXT_ENCODER.encode(JSON.stringify([rows[0]])).byteLength
    const events: WaHistorySyncStreamEventV2[] = []
    const stream = new HistorySyncStreamV2(
        async (event) => {
            events.push(event)
        },
        { ...STREAM_LIMITS, maxChunkBytes: bytes },
        new AbortController().signal
    )
    await stream.begin(MANIFEST.notification)
    for (const row of rows) await stream.append(row)
    await stream.seal(MANIFEST)
    const chunks = events.filter((event) => event.type === 'CHUNK')
    assert.equal(chunks.length, 2)
    for (const chunk of chunks) {
        assert.ok(
            TEXT_ENCODER.encode(JSON.stringify(chunk.messageObservations)).byteLength <= bytes
        )
    }
})

test('v2 rejects an oversized observation or ordinal gap; no seal or silent truncation', async () => {
    const events: WaHistorySyncStreamEventV2[] = []
    const stream = new HistorySyncStreamV2(
        async (event) => {
            events.push(event)
        },
        STREAM_LIMITS,
        new AbortController().signal
    )
    await stream.begin(MANIFEST.notification)
    await assert.rejects(stream.append(observation(2)), /not contiguous/)
    await assert.rejects(stream.append(observation(1, 'x'.repeat(8_192))), /byte limit/)
    await stream.abort()
    await stream.abort()
    assert.deepEqual(
        events.map((event) => event.type),
        ['BEGIN', 'ABORT']
    )
})

test('v2 does not start another chunk until its consumer fulfills', async () => {
    const held = deferred()
    const entered = deferred()
    const events: string[] = []
    const stream = new HistorySyncStreamV2(
        async (event) => {
            events.push(event.type)
            if (event.type === 'CHUNK') {
                entered.resolve()
                await held.promise
            }
        },
        { ...STREAM_LIMITS, maxMessagesPerChunk: 1 },
        new AbortController().signal
    )
    await stream.begin(MANIFEST.notification)
    const running = stream.append(observation(1))
    await entered.promise
    assert.deepEqual(events, ['BEGIN', 'CHUNK'])
    held.resolve()
    await running
    await stream.seal(MANIFEST)
    assert.deepEqual(events, ['BEGIN', 'CHUNK', 'SEAL'])
})

test('v2 cancellation rejects a non-cooperative callback; no concurrent ABORT or late SEAL', async () => {
    const held = deferred()
    const entered = deferred()
    const controller = new AbortController()
    const events: string[] = []
    const stream = new HistorySyncStreamV2(
        async (event) => {
            events.push(event.type)
            if (event.type === 'CHUNK') {
                entered.resolve()
                await held.promise
            }
        },
        { ...STREAM_LIMITS, maxMessagesPerChunk: 1 },
        controller.signal
    )
    await stream.begin(MANIFEST.notification)
    const running = stream.append(observation(1))
    await entered.promise
    controller.abort(new Error('synthetic cancelled'))
    await assert.rejects(running, /synthetic cancelled/)
    await stream.abort()
    held.resolve()
    await delay(0)
    assert.deepEqual(events, ['BEGIN', 'CHUNK'])
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('actual parser delivers bounded v2 chunks before verified seal and private messages', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    await processHistorySyncNotification(
        f.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(payload()) },
        options(async (event) => {
            assert.equal(f.order.includes('HIST_SYNC'), false)
            assert.equal(f.order.includes('PRIVATE_MESSAGE'), false)
            events.push(event)
        })
    )
    assert.deepEqual(
        events.map((event) => event.type),
        ['BEGIN', 'CHUNK', 'CHUNK', 'CHUNK', 'SEAL']
    )
    const seal = events.at(-1)
    assert.ok(seal?.type === 'SEAL')
    assert.equal(seal.messageObservationCount, 5)
    assert.equal(seal.manifest.parseReachedEof, true)
    assert.equal(seal.manifest.payloadIntegrityVerified, true)
    assert.equal(seal.manifest.decoded.progress, 100)
    assert.equal(seal.manifest.sourceHistoryCompleteness, 'UNKNOWN')
    assert.equal(f.order.filter((value) => value === 'PRIVATE_MESSAGE').length, 5)
    assert.equal(f.order.at(-1), 'HIST_SYNC')
})

for (const rejected of ['BEGIN', 'CHUNK', 'SEAL'] as const) {
    test(`v2 consumer rejection at ${rejected} prevents source advancement, ABORT rejection preserves cause`, async () => {
        const f = fixture()
        const original = new Error('synthetic consumer rejection')
        const events: string[] = []
        await assert.rejects(
            processHistorySyncNotification(
                f.deps,
                { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(payload()) },
                options(async (event) => {
                    events.push(event.type)
                    if (event.type === rejected) throw original
                    if (event.type === 'ABORT') throw new Error('synthetic cleanup rejection')
                })
            ),
            (error) => error === original
        )
        assert.equal(events.at(-1), 'ABORT')
        assert.equal(f.order.includes('HIST_SYNC'), false)
        assert.equal(f.order.includes('PRIVATE_MESSAGE'), false)
        assert.equal(f.order.includes('AGGREGATE'), false)
    })
}

test('v2 integrity rejection after provisional chunks cannot seal or acknowledge', async () => {
    const f = fixture()
    const verified = deferred()
    const provisional = deferred()
    const events: string[] = []
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async () => ({
            plaintext: Readable.from([gzipSync(payload())]),
            metadata: verified.promise
        })
    } as never
    const running = processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        options(async (event) => {
            events.push(event.type)
            if (event.type === 'CHUNK') provisional.resolve()
        })
    )
    await provisional.promise
    assert.equal(events.includes('SEAL'), false)
    assert.equal(f.order.includes('HIST_SYNC'), false)
    verified.reject(new Error('synthetic bad integrity'))
    await assert.rejects(running, /synthetic bad integrity/)
    assert.equal(events.at(-1), 'ABORT')
    assert.equal(events.includes('SEAL'), false)
    assert.equal(f.order.includes('PRIVATE_MESSAGE'), false)
})

test('v2 late unknown root rejects a partially staged capture rather than sealing', async () => {
    const f = fixture()
    const bytes = payload()
    const unknown = new Uint8Array(bytes.length + 3)
    unknown.set(bytes)
    unknown.set([0x98, 0x06, 0x01], bytes.length) // unknown root field 99, varint
    const events: string[] = []
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(unknown) },
            options(async (event) => {
                events.push(event.type)
            })
        ),
        /unsupported_history_root_family/
    )
    assert.ok(events.includes('CHUNK'))
    assert.equal(events.includes('SEAL'), false)
    assert.equal(events.at(-1), 'ABORT')
    assert.equal(f.order.includes('HIST_SYNC'), false)
})

test('v2 options are opt-in, mutually exclusive, finite and require both explicit budgets', () => {
    const base = { sessionId: 'synthetic', store: { session: () => ({}) } }
    const valid = {
        onStream: async () => undefined,
        streamLimits: STREAM_LIMITS,
        boundaryLimits: LIMITS
    }
    const resolve = (history: unknown) =>
        resolveWaClientBase({ ...base, history } as unknown as WaClientOptions, createNoopLogger())
    assert.doesNotThrow(() => resolve(valid))
    assert.throws(() => resolve({ ...valid, onBatch: async () => undefined }), /mutually exclusive/)
    assert.throws(() => resolve({ ...valid, enabled: false }), /enabled is false/)
    assert.throws(() => resolve({ ...valid, streamLimits: undefined }), /streamLimits are required/)
    assert.throws(
        () => resolve({ ...valid, boundaryLimits: undefined }),
        /boundaryLimits are required/
    )
    assert.throws(() => resolve({ streamLimits: STREAM_LIMITS }), /require history.onStream/)
    for (const key of Object.keys(STREAM_LIMITS)) {
        for (const value of [0, -1, 1.5, Infinity, NaN]) {
            assert.throws(
                () => resolve({ ...valid, streamLimits: { ...STREAM_LIMITS, [key]: value } }),
                /positive safe integer/
            )
        }
    }
})

test('actual WaClient v2 receipt gate admits sequential callbacks; LIVE runs while SEAL is held', async () => {
    const f = fixture()
    const entered = deferred()
    const held = deferred()
    const finished = deferred()
    const events: string[] = []
    const fake = {
        options: {
            history: {
                boundaryLimits: LIMITS,
                streamLimits: STREAM_LIMITS,
                onStream: async (event: WaHistorySyncStreamEventV2) => {
                    events.push(event.type)
                    if (event.type === 'SEAL') {
                        entered.resolve()
                        await held.promise
                        events.push('SEALED_DURABLE')
                    }
                }
            }
        },
        logger: createNoopLogger(),
        mediaTransfer: f.deps.mediaTransfer,
        stores: {},
        writeBehind: f.deps.writeBehind,
        emit: () => {
            events.push('AGGREGATE')
        },
        deps: { authClient: { getCurrentCredentials: () => null }, trustedContactToken: {} }
    }
    const prototype = WaClient.prototype as unknown as {
        processHistorySyncBoundary: (...args: unknown[]) => Promise<void>
    }
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        (...args) => prototype.processHistorySyncBoundary.call(fake, ...args),
        (reason) => assert.fail(reason)
    )
    const receipt: WaDeferredHistoryReceipt = {
        complete: () => {
            events.push('ORDINARY_RECEIPT')
        },
        fail: () => undefined,
        sendHistorySyncReceipt: async () => {
            events.push('HIST_SYNC')
        }
    }
    worker.enqueue(
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(payload()) },
        receipt,
        finished.resolve
    )
    await entered.promise
    assert.deepEqual(events, ['BEGIN', 'CHUNK', 'CHUNK', 'CHUNK', 'SEAL'])
    const encoded = proto.Message.encode({ conversation: 'synthetic live' }).finish()
    const padded = new Uint8Array(encoded.length + 1)
    padded.set(encoded)
    padded[padded.length - 1] = 1
    await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'synthetic-live', from: 'other@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            signalProtocol: { decryptMessage: async () => padded } as never,
            sendNode: async () => {
                events.push('LIVE_RECEIPT')
            },
            emitIncomingMessage: () => {
                events.push('LIVE')
            }
        }
    )
    assert.deepEqual(events.slice(-2), ['LIVE', 'LIVE_RECEIPT'])
    held.resolve()
    await finished.promise
    assert.deepEqual(events.slice(-4), [
        'SEALED_DURABLE',
        'ORDINARY_RECEIPT',
        'AGGREGATE',
        'HIST_SYNC'
    ])
    assert.equal(worker.getStatus(), 'AVAILABLE')
    await worker.cancelAndDrain()
})

test('worker faults a held later v2 callback and rejects new work; late fulfillment cannot acknowledge', async () => {
    const entered = deferred()
    const held = deferred()
    const finished = deferred()
    const failures: string[] = []
    const receipts: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, timeoutMs: 30 },
        async (_notification, signal, receipt, lifecycle) => {
            const stream = new HistorySyncStreamV2(
                async (event) => {
                    assert.equal(lifecycle.onStart(), true)
                    try {
                        if (event.type === 'CHUNK') {
                            entered.resolve()
                            await held.promise
                        }
                    } finally {
                        lifecycle.onSettled()
                    }
                    if (signal.aborted) throw signal.reason
                    if (event.type === 'SEAL') receipt.complete()
                },
                { ...STREAM_LIMITS, maxMessagesPerChunk: 1 },
                signal
            )
            await stream.begin(MANIFEST.notification)
            await stream.append(observation(1))
            await stream.seal(MANIFEST)
        },
        (reason) => {
            failures.push(reason)
        }
    )
    const receipt = {
        complete: () => {
            receipts.push('complete')
        },
        fail: () => undefined,
        sendHistorySyncReceipt: async () => undefined
    }
    worker.enqueue({ syncType: 3 }, receipt, finished.resolve)
    await entered.promise
    await delay(60) // keep the unref'd worker timer observable in this synthetic test
    await finished.promise
    assert.equal(worker.getStatus(), 'FAULTED')
    held.resolve()
    await delay(0)
    assert.deepEqual(receipts, [])
    assert.equal(
        worker.enqueue({ syncType: 3 }, receipt, () => undefined),
        false
    )
    assert.ok(failures.includes('timeout'))
    assert.ok(failures.includes('faulted'))
    await worker.cancelAndDrain()
})
