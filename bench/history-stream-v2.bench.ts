/** Synthetic source-memory probe. No WhatsApp account, store, DB, downloads or private logs. */
import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import { setImmediate as yieldImmediate } from 'node:timers/promises'
import { createGzip } from 'node:zlib'

import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { createNoopLogger } from '@infra/log/types'
import { proto } from '@proto'
import { concatBytes } from '@util/bytes'

async function run(): Promise<void> {
    const count = Number(process.argv[2])
    assert.ok(Number.isSafeInteger(count) && count > 0 && count <= 1_000_000)
    const address = '15550000001@s.whatsapp.net'
    const header = proto.Conversation.encode({ id: address }).finish()
    const record = proto.HistorySyncMsg.encode({
        message: {
            key: { remoteJid: address, id: 'synthetic-repeated-id', fromMe: false },
            messageTimestamp: 1_700_000_000,
            message: { conversation: 'synthetic bounded-source probe' }
        }
    }).finish()
    // Repeated source occurrences are intentional: the source emitter must not deduplicate.
    const framedRecord = proto.Conversation.encode({}).uint32(18).bytes(record).finish()
    const conversationBytes = header.length + count * framedRecord.length
    const prefix = proto.HistorySync.encode({ syncType: 3 })
        .uint32(18)
        .uint32(conversationBytes)
        .finish()
    async function* generate() {
        yield concatBytes([prefix, header])
        const block = concatBytes(Array.from({ length: 128 }, () => framedRecord))
        for (let offset = 0; offset < count; offset += 128)
            yield block.subarray(0, Math.min(128, count - offset) * framedRecord.length)
    }
    global.gc?.()
    const baseline = process.memoryUsage()
    let peakRss = baseline.rss
    let peakHeap = baseline.heapUsed
    let processed = 0
    let chunks = 0
    let maxChunkRows = 0
    let maxFactRows = 0
    let sealed = false
    let privateWrites = 0
    let liveControlTicks = 0
    const started = performance.now()
    const sample = () => {
        const memory = process.memoryUsage()
        peakRss = Math.max(peakRss, memory.rss)
        peakHeap = Math.max(peakHeap, memory.heapUsed)
    }
    const sampler = setInterval(sample, 5)
    const liveControl = setInterval(() => {
        liveControlTicks++
    }, 5)
    try {
        await processHistorySyncNotification(
            {
                logger: createNoopLogger(),
                mediaTransfer: {
                    downloadAndDecryptStream: async () => ({
                        plaintext: Readable.from(generate()).pipe(createGzip()),
                        metadata: Promise.resolve({})
                    })
                } as never,
                writeBehind: {
                    ownHistoryOperation: (start: () => Promise<void>) => start(),
                    historyOperationsSettled: () => true,
                    persistMessageAsync: async () => {
                        privateWrites++
                    },
                    persistThreadAsync: async () => {
                        privateWrites++
                    },
                    persistContactAsync: async () => {
                        privateWrites++
                    }
                } as never,
                emitEvent: () => undefined
            },
            {
                syncType: 3,
                directPath: '/synthetic-only',
                mediaKey: new Uint8Array(32),
                fileSha256: new Uint8Array(32),
                fileEncSha256: new Uint8Array(32)
            },
            {
                signal: new AbortController().signal,
                streamContent: 'RICH_V2',
                streamFacts: 'EXACT_V1',
                privateMailbox: 'DISABLED',
                limits: {
                    maxConcurrentBatches: 1,
                    maxQueuedBatches: 1,
                    maxQueuedBytes: 65_536,
                    maxBatchBytes: conversationBytes + 65_536,
                    maxMessagesPerBatch: count,
                    maxSourceRecordsPerBatch: count * 32 + 128,
                    timeoutMs: 180_000
                },
                streamLimits: {
                    maxMessagesPerChunk: 128,
                    maxChunkBytes: 65_536,
                    maxParkedMessages: 2,
                    facts: { maxFactsPerChunk: 16, maxFactChunkBytes: 8_192 },
                    resources: {
                        maxRecordBytes: 4_096,
                        maxConversationHeaderBytes: 4_096,
                        maxParkedBytes: 8_192,
                        maxRetainedPrivateBytes: 8_192,
                        maxRetainedNotificationBytes: 8_192,
                        maxThumbnailBytes: 1_024
                    }
                },
                onStream: async (event) => {
                    if (event.type === 'CHUNK') {
                        chunks++
                        processed += event.messageObservations.length
                        maxChunkRows = Math.max(maxChunkRows, event.messageObservations.length)
                    } else if (event.type === 'FACT_CHUNK') {
                        maxFactRows = Math.max(maxFactRows, event.sourceFacts.length)
                    } else if (event.type === 'SEAL') {
                        assert.equal(event.messageObservationCount, count)
                        assert.equal(event.factObservationCount, 1)
                        sealed = true
                    }
                    sample()
                    // A CPU-only synthetic consumer must yield; real durable staging yields through bounded DB IO.
                    await yieldImmediate()
                }
            }
        )
        sample()
        assert.equal(processed, count)
        assert.equal(privateWrites, 0)
        assert.equal(sealed, true)
        console.log(
            JSON.stringify({
                mode: 'SYNTHETIC_SOURCE_MEMORY_ONLY',
                count,
                processed,
                chunks,
                maxChunkRows,
                maxFactRows,
                privateWrites,
                baselineRssBytes: baseline.rss,
                peakRssBytes: peakRss,
                rssGrowthBytes: peakRss - baseline.rss,
                peakHeapBytes: peakHeap,
                elapsedMs: Math.round(performance.now() - started),
                liveControlTicks,
                sourceInflatedBytes: prefix.length + conversationBytes,
                sourceCompleteness: 'UNKNOWN',
                processHighWaterRssBytes:
                    process.resourceUsage().maxRSS > 0
                        ? process.resourceUsage().maxRSS * 1_024
                        : null,
                rssSamplingMs: 5,
                realWhatsApp: false,
                durableStaging: false,
                mediaRetrieval: false
            })
        )
    } finally {
        clearInterval(sampler)
        clearInterval(liveControl)
    }
}

void run().catch(() => {
    console.error('SYNTHETIC_SOURCE_MEMORY_PROBE_FAILED')
    process.exitCode = 1
})
