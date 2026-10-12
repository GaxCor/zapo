import { randomUUID } from 'node:crypto'

import type {
    WaHistorySyncBatchV1,
    WaHistorySyncMessageObservation,
    WaHistorySyncStreamEventV2,
    WaHistorySyncStreamHandler,
    WaHistorySyncStreamLimits
} from '@client/types'
import { TEXT_ENCODER } from '@util/bytes'

/** One sequential provisional capture; no full-batch observation collection. */
export class HistorySyncStreamV2 {
    private readonly captureId = randomUUID()
    private pending: WaHistorySyncMessageObservation[] = []
    private pendingBytes = 2
    private chunkCount = 0
    private messageCount = 0
    private began = false
    private terminal = false

    public constructor(
        private readonly handler: WaHistorySyncStreamHandler,
        private readonly limits: WaHistorySyncStreamLimits,
        private readonly signal: AbortSignal
    ) {}

    public async begin(notification: WaHistorySyncBatchV1['notification']): Promise<void> {
        if (this.began || this.terminal) throw new Error('history stream already began')
        this.began = true
        await this.dispatch({
            version: 2,
            type: 'BEGIN',
            captureId: this.captureId,
            sourceHistoryCompleteness: 'UNKNOWN',
            notification
        })
    }

    public async append(observation: WaHistorySyncMessageObservation): Promise<void> {
        this.assertBuilding()
        if (observation.ordinal !== this.messageCount + 1) {
            throw new Error('history stream observation ordinal is not contiguous')
        }
        const bytes = TEXT_ENCODER.encode(JSON.stringify(observation)).byteLength
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

    /** Only the parser may call this after EOF, integrity verification and all source gates. */
    public async seal(
        manifest: Omit<WaHistorySyncBatchV1, 'version' | 'messageObservations'>
    ): Promise<void> {
        this.assertBuilding()
        await this.flush()
        await this.dispatch({
            version: 2,
            type: 'SEAL',
            captureId: this.captureId,
            chunkCount: this.chunkCount,
            messageObservationCount: this.messageCount,
            manifest
        })
        this.terminal = true
    }

    /** Never races an unresolved callback on cancellation; the durable consumer recovers BUILDING. */
    public async abort(): Promise<void> {
        this.pending = []
        this.pendingBytes = 2
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
