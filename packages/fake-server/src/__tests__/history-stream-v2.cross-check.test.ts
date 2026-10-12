import assert from 'node:assert/strict'
import test from 'node:test'
import { gzipSync } from 'node:zlib'

import {
    createNoopLogger,
    createStore,
    proto,
    WaClient,
    type WaHistorySyncStreamEventV2
} from 'zapo-js'

import { FakeWaServer } from '../api/FakeWaServer'
import { parsePairingQrString } from '../protocol/auth/pair-device'

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

for (const profile of ['TEXT_V1', 'RICH_V2', 'RICH_FACTS', 'SELF_FACTS'] as const) {
    const rich = profile !== 'TEXT_V1'
    const facts = profile === 'RICH_FACTS' || profile === 'SELF_FACTS'
    const selfSource = profile === 'SELF_FACTS'
    test(
        `actual local encrypted ${profile} flow preserves LIVE and gates receipts on SEAL`,
        { timeout: 20_000 },
        async () => {
            const server = await FakeWaServer.start()
            const store = createStore({})
            const chunkHeld = deferred()
            const chunkEntered = deferred()
            const sealHeld = deferred()
            const sealEntered = deferred()
            const liveObserved = deferred()
            const liveFactObserved = deferred()
            const factEntered = deferred()
            const factHeld = deferred()
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
                if (
                    node.attrs.id === 'synthetic-history-envelope' &&
                    node.attrs.type !== 'hist_sync'
                ) {
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
                        ...(facts ? { streamFacts: 'EXACT_V1' as const } : {}),
                        ...(rich
                            ? {
                                  streamContent: 'RICH_V2' as const,
                                  privateMailbox: 'DISABLED' as const
                              }
                            : {}),
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
                            maxParkedMessages: 2,
                            ...(facts
                                ? { facts: { maxFactsPerChunk: 1, maxFactChunkBytes: 4_096 } }
                                : {}),
                            ...(rich
                                ? {
                                      resources: {
                                          maxRecordBytes: 8_192,
                                          maxConversationHeaderBytes: 4_096,
                                          maxParkedBytes: 8_192,
                                          maxRetainedPrivateBytes: 8_192,
                                          maxRetainedNotificationBytes: 16_384,
                                          maxThumbnailBytes: 1_024
                                      }
                                  }
                                : {})
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
                            if (event.type === 'FACT_CHUNK' && event.sequence === 1) {
                                factEntered.resolve()
                                await factHeld.promise
                            }
                        }
                    }
                },
                createNoopLogger()
            )
            // Local FakeWaServer pairing only: memory-only synthetic material,
            // never rendered, printed, saved or taken from a real account.
            const paired = deferred()
            const material = selfSource
                ? new Promise<ReturnType<typeof parsePairingQrString>>((resolve) => {
                      client.once('auth_qr', (event) => resolve(parsePairingQrString(event.qr)))
                      client.once('auth_paired', () => paired.resolve())
                  })
                : undefined
            client.on('message', (event) => {
                liveMessageCount++
                if (event.key.id === 'synthetic-live-envelope') liveObserved.resolve()
                if (event.key.id === 'synthetic-live-facts') liveFactObserved.resolve()
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
                let pipeline = await server.waitForAuthenticatedPipeline()
                if (selfSource) {
                    const reconnected = server.waitForNextAuthenticatedPipeline()
                    await server.runPairing(
                        pipeline,
                        { deviceJid: '5511999999999:1@s.whatsapp.net' },
                        () => material!
                    )
                    await within(paired.promise, 'synthetic pairing')
                    pipeline = await reconnected
                }
                await server.triggerPreKeyUpload(pipeline)
                // All credentials in this test belong to the local synthetic server.
                const syntheticSelf = selfSource
                    ? client.getCredentials()!.meJid!.replace(/:\d+@/, '@')
                    : undefined
                const peer = await server.createFakePeer(
                    { jid: selfSource ? syntheticSelf! : '15551111111@s.whatsapp.net' },
                    pipeline
                )
                const contact = await server.createFakePeer(
                    { jid: '15553333333@s.whatsapp.net' },
                    pipeline
                )
                const media = {
                    mimetype: 'image/jpeg',
                    directPath: '/synthetic.enc',
                    mediaKey: new Uint8Array(32).fill(1),
                    fileSha256: new Uint8Array(32).fill(2),
                    fileEncSha256: new Uint8Array(32).fill(3)
                }
                const messages = rich
                    ? [
                          { imageMessage: media },
                          {
                              audioMessage: {
                                  ...media,
                                  mimetype: 'audio/ogg',
                                  ptt: true,
                                  seconds: 2
                              }
                          },
                          { locationMessage: { degreesLatitude: 0, degreesLongitude: 0 } },
                          {
                              extendedTextMessage: {
                                  text: 'synthetic',
                                  contextInfo: {
                                      isForwarded: true,
                                      forwardingScore: 1,
                                      stanzaId: 'synthetic-quote',
                                      quotedMessage: { conversation: 'synthetic quote' }
                                  }
                              }
                          },
                          {
                              documentMessage: {
                                  ...media,
                                  mimetype: 'application/pdf',
                                  fileName: 'synthetic.pdf'
                              }
                          }
                      ]
                    : Array.from({ length: 5 }, () => ({ conversation: 'synthetic history' }))
                if (facts) {
                    const bytes = proto.HistorySync.encode({
                        syncType: 0,
                        progress: 100,
                        conversations: [
                            {
                                id: '15552222222@s.whatsapp.net',
                                messages: messages.map((message, index) => ({
                                    message: {
                                        key: {
                                            id: `synthetic-history-${index}`,
                                            fromMe: false,
                                            remoteJid: '15552222222@s.whatsapp.net'
                                        },
                                        messageTimestamp: 1_700_000_000 + index,
                                        message
                                    }
                                }))
                            }
                        ],
                        phoneNumberToLidMappings: [
                            { pnJid: '15552222222@s.whatsapp.net', lidJid: '100000000000001@lid' },
                            { pnJid: '15552222222@s.whatsapp.net', lidJid: '100000000000002@lid' }
                        ]
                    }).finish()
                    await peer.sendMessage(
                        {
                            protocolMessage: {
                                type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                                historySyncNotification: {
                                    syncType: 0,
                                    progress: 100,
                                    initialHistBootstrapInlinePayload: gzipSync(bytes)
                                }
                            }
                        },
                        { id: 'synthetic-history-envelope' }
                    )
                } else
                    await peer.sendHistorySync(
                        {
                            progress: 100,
                            conversations: [
                                {
                                    id: '15552222222@s.whatsapp.net',
                                    messages: messages.map((message, index) => ({
                                        id: `synthetic-history-${index}`,
                                        fromMe: false,
                                        timestamp: 1_700_000_000 + index,
                                        message
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
                if (facts) {
                    await within(factEntered.promise, 'held fact chunk')
                    assert.equal(ordinaryReceipts, 0)
                    assert.equal(histReceipts, 0)
                    await contact.sendMessage(
                        { conversation: 'synthetic live facts' },
                        { id: 'synthetic-live-facts' }
                    )
                    await within(liveFactObserved.promise, 'live beside fact chunk')
                    assert.equal(ordinaryReceipts, 0)
                    factHeld.resolve()
                }
                await within(sealEntered.promise, 'seal')
                assert.deepEqual(
                    events
                        .filter((event) => event.type !== 'FACT_CHUNK')
                        .map((event) => event.type),
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
                assert.equal(seal.contentProfile, rich ? 'RICH_V2' : undefined)
                assert.equal(seal.factProfile, facts ? 'EXACT_V1' : undefined)
                assert.equal(
                    seal.sourceEnvelope?.origin,
                    facts
                        ? selfSource
                            ? 'SELF_ACCOUNT_INCOMING_KEY'
                            : 'OTHER_ACCOUNT_INCOMING_KEY'
                        : undefined
                )
                if (facts)
                    assert.equal(seal.sourceEnvelope?.sourceMessageId, 'synthetic-history-envelope')
                if (facts) {
                    const captured = events.flatMap((event) =>
                        event.type === 'FACT_CHUNK' ? [...event.sourceFacts] : []
                    )
                    assert.equal(seal.factObservationCount, 3)
                    assert.equal(
                        captured.filter((fact) => fact.kind === 'IDENTITY_ASSERTION').length,
                        2
                    )
                    assert.ok(
                        captured.every(
                            (fact) =>
                                fact.kind !== 'IDENTITY_ASSERTION' ||
                                fact.resolution === 'UNRESOLVED'
                        )
                    )
                }
                const rows = events.flatMap((event) =>
                    event.type === 'CHUNK' ? [...event.messageObservations] : []
                )
                assert.deepEqual(
                    rows.map((row) => row.content.kind),
                    rich
                        ? ['IMAGE', 'VOICE_NOTE', 'LOCATION', 'TEXT', 'DOCUMENT']
                        : Array.from({ length: 5 }, () => 'TEXT')
                )
                if (rich) {
                    assert.ok(rows[0].mediaSource)
                    assert.throws(() => JSON.stringify(rows[0]), /protected persistence/)
                    assert.equal(rows[3].context?.isForwarded, true)
                    assert.equal(rows[3].context?.quote?.externalMessageId, 'synthetic-quote')
                }
            } finally {
                chunkHeld.resolve()
                sealHeld.resolve()
                factHeld.resolve()
                unsubscribe()
                await client.disconnect()
                await store.destroy()
                await server.stop()
            }
        }
    )
}
