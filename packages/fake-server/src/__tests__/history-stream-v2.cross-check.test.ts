import assert from 'node:assert/strict'
import test from 'node:test'

import { createNoopLogger, createStore, WaClient, type WaHistorySyncStreamEventV2 } from 'zapo-js'

import { FakeWaServer } from '../api/FakeWaServer'

function deferred() {
    let resolve!: () => void
    const promise = new Promise<void>((done) => {
        resolve = done
    })
    return { promise, resolve }
}

async function within(promise: Promise<void>, stage: string | (() => string)): Promise<void> {
    let timer!: NodeJS.Timeout
    try {
        await Promise.race([
            promise,
            new Promise<never>((_resolve, reject) => {
                timer = setTimeout(
                    () =>
                        reject(
                            new Error(
                                `synthetic timeout: ${typeof stage === 'function' ? stage() : stage}`
                            )
                        ),
                    5_000
                )
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

test(
    'actual local encrypted client flow streams provisional history, preserves LIVE and gates receipts on SEAL',
    { timeout: 20_000 },
    async () => {
        const server = await FakeWaServer.start()
        const store = createStore({})
        const chunkHeld = deferred()
        const chunkEntered = deferred()
        const sealHeld = deferred()
        const sealEntered = deferred()
        const liveObserved = deferred()
        const historyReceipt = deferred()
        const histReceipt = deferred()
        const processed = deferred()
        const events: WaHistorySyncStreamEventV2[] = []
        let ordinaryReceipts = 0
        let histReceipts = 0
        let liveMessageCount = 0
        let unhandledCount = 0
        const failures: string[] = []
        const unsubscribe = server.onCapturedStanza((node) => {
            if (node.tag !== 'receipt') return
            if (node.attrs.id === 'synthetic-history-envelope' && node.attrs.type !== 'hist_sync') {
                ordinaryReceipts++
                historyReceipt.resolve()
            }
            if (node.attrs.type === 'hist_sync') {
                histReceipts++
                histReceipt.resolve()
            }
        })
        const client = new WaClient(
            {
                store,
                sessionId: 'synthetic-stream-v2',
                connectTimeoutMs: 5_000,
                chatSocketUrls: [server.url],
                testHooks: { noiseRootCa: server.noiseRootCa },
                history: {
                    enabled: true,
                    groupBundles: false,
                    boundaryLimits: {
                        maxConcurrentBatches: 1,
                        maxQueuedBatches: 1,
                        maxQueuedBytes: 65_536,
                        maxBatchBytes: 65_536,
                        maxMessagesPerBatch: 32,
                        maxSourceRecordsPerBatch: 8_192,
                        timeoutMs: 15_000
                    },
                    streamLimits: {
                        maxMessagesPerChunk: 2,
                        maxChunkBytes: 4_096,
                        maxParkedMessages: 2
                    },
                    onStream: async (event) => {
                        events.push(event)
                        if (event.type === 'CHUNK' && event.sequence === 1) {
                            chunkEntered.resolve()
                            await chunkHeld.promise
                        }
                        if (event.type === 'SEAL') {
                            sealEntered.resolve()
                            await sealHeld.promise
                        }
                    }
                }
            },
            createNoopLogger()
        )
        client.on('message', (event) => {
            liveMessageCount++
            if (event.key.id === 'synthetic-live-envelope') liveObserved.resolve()
        })
        client.on('debug_unhandled_stanza', () => {
            unhandledCount++
        })
        client.on('history_sync_chunk', () => processed.resolve())
        client.on('history_sync_boundary_failure', (event) => {
            failures.push(event.reason)
        })
        try {
            await client.connect()
            const pipeline = await server.waitForAuthenticatedPipeline()
            await server.triggerPreKeyUpload(pipeline)
            const peer = await server.createFakePeer(
                { jid: '15551111111@s.whatsapp.net' },
                pipeline
            )
            const contact = await server.createFakePeer(
                { jid: '15553333333@s.whatsapp.net' },
                pipeline
            )
            await peer.sendHistorySync(
                {
                    progress: 100,
                    conversations: [
                        {
                            id: '15552222222@s.whatsapp.net',
                            messages: Array.from({ length: 5 }, (_, index) => ({
                                id: `synthetic-history-${index}`,
                                fromMe: false,
                                timestamp: 1_700_000_000 + index,
                                message: { conversation: 'synthetic history' }
                            }))
                        }
                    ]
                },
                { id: 'synthetic-history-envelope' }
            )
            await within(chunkEntered.promise, 'first chunk')
            assert.deepEqual(
                events.map((event) => event.type),
                ['BEGIN', 'CHUNK']
            )
            assert.equal(ordinaryReceipts, 0)
            assert.equal(histReceipts, 0)
            await contact.sendMessage(
                { conversation: 'synthetic live' },
                { id: 'synthetic-live-envelope' }
            )
            await within(
                liveObserved.promise,
                () =>
                    `live message (messages=${liveMessageCount}, unhandled=${unhandledCount}, failures=${failures.length})`
            )
            assert.equal(ordinaryReceipts, 0)
            chunkHeld.resolve()
            await within(sealEntered.promise, 'seal')
            assert.deepEqual(
                events.map((event) => event.type),
                ['BEGIN', 'CHUNK', 'CHUNK', 'CHUNK', 'SEAL']
            )
            assert.equal(ordinaryReceipts, 0)
            assert.equal(histReceipts, 0)
            sealHeld.resolve()
            await within(historyReceipt.promise, 'ordinary receipt')
            await within(processed.promise, 'processed chunk')
            await within(histReceipt.promise, 'hist_sync receipt')
            assert.equal(ordinaryReceipts, 1)
            assert.equal(histReceipts, 1)
            assert.deepEqual(failures, [])
            const seal = events.at(-1)
            assert.ok(seal?.type === 'SEAL')
            assert.equal(seal.messageObservationCount, 5)
            assert.equal(seal.manifest.payloadIntegrityVerified, true)
            assert.equal(seal.manifest.sourceHistoryCompleteness, 'UNKNOWN')
        } finally {
            chunkHeld.resolve()
            sealHeld.resolve()
            unsubscribe()
            await client.disconnect()
            await store.destroy()
            await server.stop()
        }
    }
)
