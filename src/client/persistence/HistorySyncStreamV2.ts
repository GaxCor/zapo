import { randomUUID } from 'node:crypto'

import type { HistorySourceFactInput } from '@client/persistence/history-facts-v2'
import type {
    WaHistorySyncBatchV1,
    WaHistorySyncMessageObservationV2,
    WaHistorySyncSourceEnvelopeV2,
    WaHistorySyncSourceFactV2,
    WaHistorySyncStreamEventV2,
    WaHistorySyncStreamHandler,
    WaHistorySyncStreamLimits
} from '@client/types'
import { TEXT_ENCODER } from '@util/bytes'

/** One sequential provisional capture; no full-batch observation collection. */
export class HistorySyncStreamV2 {
    private readonly captureId = randomUUID()
    private pending: WaHistorySyncMessageObservationV2[] = []
    private pendingBytes = 2
    private chunkCount = 0
    private messageCount = 0
    private facts: WaHistorySyncSourceFactV2[] = []
    private factBytes = 2
    private factCount = 0
    private factChunks = 0
    private began = false
    private terminal = false

    public constructor(
        private readonly handler: WaHistorySyncStreamHandler,
        private readonly limits: WaHistorySyncStreamLimits,
        private readonly signal: AbortSignal,
        private readonly contentProfile: 'TEXT_V1' | 'RICH_V2' = 'TEXT_V1',
        private readonly factProfile?: 'EXACT_V1',
        private readonly sourceEnvelope: WaHistorySyncSourceEnvelopeV2 = {
            origin: 'UNVERIFIED_SOURCE'
        }
    ) {
        // Callback code must not mutate envelope provenance between BEGIN and SEAL.
        this.sourceEnvelope = Object.freeze({ ...sourceEnvelope })
    }

    public async begin(notification: WaHistorySyncBatchV1['notification']): Promise<void> {
        if (this.began || this.terminal) throw new Error('history stream already began')
        this.began = true
        await this.dispatch({
            version: 2,
            type: 'BEGIN',
            captureId: this.captureId,
            sourceHistoryCompleteness: 'UNKNOWN',
            notification,
            ...(this.contentProfile === 'RICH_V2' ? { contentProfile: 'RICH_V2' as const } : {}),
            ...(this.factProfile
                ? { factProfile: this.factProfile, sourceEnvelope: this.sourceEnvelope }
                : {})
        })
    }

    public async append(observation: WaHistorySyncMessageObservationV2): Promise<void> {
        this.assertBuilding()
        if (observation.ordinal !== this.messageCount + 1) {
            throw new Error('history stream observation ordinal is not contiguous')
        }
        // Protected handles reject generic JSON. Charge them separately without
        // copying keys/locators into a temporary serialized object or diagnostics.
        const { mediaSource, ...metadata } = observation
        const bytes =
            TEXT_ENCODER.encode(JSON.stringify(metadata)).byteLength +
            (mediaSource?.getRetainedByteLength() ?? 0)
        if (bytes + 2 > this.limits.maxChunkBytes) {
            throw new Error('history stream observation exceeds configured chunk byte limit')
        }
        if (this.pendingBytes + bytes + (this.pending.length ? 1 : 0) > this.limits.maxChunkBytes) {
            await this.flush()
        }
        this.pendingBytes += bytes + (this.pending.length ? 1 : 0)
        this.pending.push(observation)
        this.messageCount += 1
        if (this.pending.length >= this.limits.maxMessagesPerChunk) await this.flush()
    }

    public async appendFact(fact: HistorySourceFactInput): Promise<void> {
        this.assertBuilding()
        if (!this.factProfile || !this.limits.facts) throw new Error('history facts not configured')
        const row = { ...fact, ordinal: this.factCount + 1 } as WaHistorySyncSourceFactV2
        const bytes = TEXT_ENCODER.encode(JSON.stringify(row)).byteLength
        if (bytes + 2 > this.limits.facts.maxFactChunkBytes)
            throw new Error('history fact exceeds configured fact-chunk byte limit')
        if (
            this.factBytes + bytes + (this.facts.length ? 1 : 0) >
            this.limits.facts.maxFactChunkBytes
        )
            await this.flushFacts()
        this.factBytes += bytes + (this.facts.length ? 1 : 0)
        this.facts.push(row)
        this.factCount++
        if (this.facts.length >= this.limits.facts.maxFactsPerChunk) await this.flushFacts()
    }

    /** Only the parser may call this after EOF, integrity verification and all source gates. */
    public async seal(
        manifest: Omit<WaHistorySyncBatchV1, 'version' | 'messageObservations'>
    ): Promise<void> {
        this.assertBuilding()
        await this.flush()
        await this.flushFacts()
        await this.dispatch({
            version: 2,
            type: 'SEAL',
            captureId: this.captureId,
            chunkCount: this.chunkCount,
            messageObservationCount: this.messageCount,
            manifest,
            ...(this.contentProfile === 'RICH_V2' ? { contentProfile: 'RICH_V2' as const } : {}),
            ...(this.factProfile
                ? {
                      factProfile: this.factProfile,
                      factChunkCount: this.factChunks,
                      factObservationCount: this.factCount,
                      sourceEnvelope: this.sourceEnvelope
                  }
                : {})
        })
        this.terminal = true
    }

    /** Never races an unresolved callback on cancellation; the durable consumer recovers BUILDING. */
    public async abort(): Promise<void> {
        this.pending = []
        this.pendingBytes = 2
        this.facts = []
        this.factBytes = 2
        if (!this.began || this.terminal) return
        this.terminal = true
        if (this.signal.aborted) return
        await this.dispatch({
            version: 2,
            type: 'ABORT',
            captureId: this.captureId,
            reason: 'capture_failed'
        })
    }

    private assertBuilding(): void {
        if (!this.began || this.terminal) throw new Error('history stream is not building')
        this.throwIfAborted()
    }

    private async flush(): Promise<void> {
        if (this.pending.length === 0) return
        const observations = this.pending
        this.pending = []
        this.pendingBytes = 2
        await this.dispatch({
            version: 2,
            type: 'CHUNK',
            captureId: this.captureId,
            sequence: this.chunkCount + 1,
            messageObservations: observations
        })
        this.chunkCount += 1
    }

    private async flushFacts(): Promise<void> {
        if (!this.facts.length) return
        const facts = this.facts
        this.facts = []
        this.factBytes = 2
        await this.dispatch({
            version: 2,
            type: 'FACT_CHUNK',
            captureId: this.captureId,
            sequence: this.factChunks + 1,
            sourceFacts: facts
        })
        this.factChunks++
    }

    private throwIfAborted(): void {
        if (this.signal.aborted) {
            throw this.signal.reason instanceof Error
                ? this.signal.reason
                : new Error('history stream cancelled')
        }
    }

    private async dispatch(event: WaHistorySyncStreamEventV2): Promise<void> {
        this.throwIfAborted()
        await new Promise<void>((resolve, reject) => {
            const onAbort = (): void => {
                this.signal.removeEventListener('abort', onAbort)
                reject(
                    this.signal.reason instanceof Error
                        ? this.signal.reason
                        : new Error('history stream cancelled')
                )
            }
            this.signal.addEventListener('abort', onAbort, { once: true })
            Promise.resolve()
                .then(() => {
                    this.throwIfAborted()
                    return this.handler(event, { signal: this.signal })
                })
                .then(resolve, reject)
                .finally(() => this.signal.removeEventListener('abort', onAbort))
        })
        this.throwIfAborted()
    }
}
