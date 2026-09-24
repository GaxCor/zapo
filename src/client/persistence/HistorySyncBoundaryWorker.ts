import type { WaHistorySyncBoundaryLimits } from '@client/types'
import type { WaDeferredHistoryReceipt } from '@message/primitives/incoming'
import type { Proto } from '@proto'

interface QueuedHistoryWork {
    readonly notification: Proto.Message.IHistorySyncNotification
    readonly receipt: WaDeferredHistoryReceipt
    readonly onSettled: () => void
    readonly retainedBytes: number
}

interface ActiveHistoryWork extends QueuedHistoryWork {
    readonly controller: AbortController
    timeout: ReturnType<typeof setTimeout> | null
}

export type ProcessHistorySyncWork = (
    notification: Proto.Message.IHistorySyncNotification,
    signal: AbortSignal,
    receipt: WaDeferredHistoryReceipt
) => Promise<void>

/** Per-client bounded executor for the optional durable-history boundary. */
export class HistorySyncBoundaryWorker {
    private readonly limits: WaHistorySyncBoundaryLimits
    private readonly process: ProcessHistorySyncWork
    private readonly onFailure: (
        reason: 'queue_full' | 'processing_failed' | 'timeout' | 'cancelled'
    ) => void
    private readonly queue: QueuedHistoryWork[] = []
    private readonly active = new Map<ActiveHistoryWork, true>()
    private queuedBytes = 0
    private readonly drainWaiters: Array<() => void> = []

    public constructor(
        limits: WaHistorySyncBoundaryLimits,
        process: ProcessHistorySyncWork,
        onFailure: (reason: 'queue_full' | 'processing_failed' | 'timeout' | 'cancelled') => void
    ) {
        this.limits = limits
        this.process = process
        this.onFailure = onFailure
    }

    /** Synchronous admission only; payload work always starts in a later microtask. */
    public enqueue(
        notification: Proto.Message.IHistorySyncNotification,
        receipt: WaDeferredHistoryReceipt,
        onSettled: () => void
    ): boolean {
        const retainedBytes = estimateRetainedBytes(notification)
        const sourceBytes = readDeclaredLength(notification)
        if (
            retainedBytes === null ||
            sourceBytes === null ||
            sourceBytes > this.limits.maxBatchBytes ||
            (notification.initialHistBootstrapInlinePayload instanceof Uint8Array &&
                notification.initialHistBootstrapInlinePayload.byteLength >
                    this.limits.maxBatchBytes)
        ) {
            receipt.fail()
            onSettled()
            this.onFailure('processing_failed')
            return false
        }

        const work: QueuedHistoryWork = {
            notification,
            receipt,
            onSettled,
            retainedBytes
        }

        if (this.active.size < this.limits.maxConcurrentBatches) {
            this.start(work)
            return true
        }

        if (
            this.queue.length >= this.limits.maxQueuedBatches ||
            this.queuedBytes + retainedBytes > this.limits.maxQueuedBytes
        ) {
            receipt.fail()
            onSettled()
            this.onFailure('queue_full')
            return false
        }

        this.queue.push(work)
        this.queuedBytes += retainedBytes
        return true
    }

    /** Cancels queued/active work and waits for abort-aware active work to unwind. */
    public async cancelAndDrain(
        reason: unknown = new Error('history worker stopped')
    ): Promise<void> {
        while (this.queue.length > 0) {
            const work = this.queue.shift()!
            this.queuedBytes -= work.retainedBytes
            work.receipt.fail()
            work.onSettled()
            this.onFailure('cancelled')
        }
        for (const work of this.active.keys()) {
            work.receipt.fail()
            work.controller.abort(reason)
            this.onFailure('cancelled')
        }
        if (this.active.size === 0) {
            return
        }
        await new Promise<void>((resolve) => this.drainWaiters.push(resolve))
    }

    public queued(): number {
        return this.queue.length
    }

    public running(): number {
        return this.active.size
    }

    private start(work: QueuedHistoryWork): void {
        const activeWork: ActiveHistoryWork = {
            ...work,
            controller: new AbortController(),
            timeout: null
        }
        this.active.set(activeWork, true)
        activeWork.timeout = setTimeout(() => {
            activeWork.receipt.fail()
            activeWork.controller.abort(new Error('history boundary processing timed out'))
            this.onFailure('timeout')
        }, this.limits.timeoutMs)
        activeWork.timeout.unref?.()
        queueMicrotask(() => {
            void this.run(activeWork)
        })
    }

    private async run(work: ActiveHistoryWork): Promise<void> {
        try {
            await this.process(work.notification, work.controller.signal, work.receipt)
        } catch {
            work.receipt.fail()
            if (!work.controller.signal.aborted) {
                this.onFailure('processing_failed')
            }
        } finally {
            work.receipt.fail()
            if (work.timeout) {
                clearTimeout(work.timeout)
            }
            this.active.delete(work)
            work.onSettled()
            this.startQueued()
            this.resolveDrainWaiters()
        }
    }

    private startQueued(): void {
        while (this.active.size < this.limits.maxConcurrentBatches && this.queue.length > 0) {
            const work = this.queue.shift()!
            this.queuedBytes -= work.retainedBytes
            this.start(work)
        }
    }

    private resolveDrainWaiters(): void {
        if (this.active.size !== 0 || this.drainWaiters.length === 0) {
            return
        }
        const waiters = this.drainWaiters.splice(0)
        for (const resolve of waiters) {
            resolve()
        }
    }
}

function estimateRetainedBytes(
    notification: Proto.Message.IHistorySyncNotification
): number | null {
    let size = 256
    const add = (value: Uint8Array | string | null | undefined): void => {
        if (value instanceof Uint8Array) {
            size += value.byteLength
        } else if (typeof value === 'string') {
            size += value.length * 2
        }
    }
    add(notification.initialHistBootstrapInlinePayload)
    add(notification.directPath)
    add(notification.mediaKey)
    add(notification.fileSha256)
    add(notification.fileEncSha256)
    return Number.isSafeInteger(size) ? size : null
}

function readDeclaredLength(notification: Proto.Message.IHistorySyncNotification): number | null {
    const value = notification.fileLength
    if (value === null || value === undefined) {
        return notification.initialHistBootstrapInlinePayload ? 0 : 0
    }
    const parsed = Number(typeof value === 'number' ? value : value.toString())
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null
}
