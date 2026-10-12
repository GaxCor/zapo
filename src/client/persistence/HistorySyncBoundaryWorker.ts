import { performance } from 'node:perf_hooks'

import type {
    WaHistorySyncBoundaryFaultReason,
    WaHistorySyncBoundaryLimits,
    WaHistorySyncBoundaryState
} from '@client/types'
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
    readonly deadlineAt: number
    timeout: ReturnType<typeof setTimeout> | null
    callbackStarted: boolean
    callbackPending: boolean
    timedOut: boolean
}

export interface HistorySyncCallbackLifecycle {
    /** Returns false when the deadline/fault has already made callback admission terminal. */
    readonly onStart: () => boolean
    /** Observes settlement without retaining the promise in a worker-owned collection. */
    readonly onSettled: () => void
}

export type ProcessHistorySyncWork = (
    notification: Proto.Message.IHistorySyncNotification,
    signal: AbortSignal,
    receipt: WaDeferredHistoryReceipt,
    callbackLifecycle: HistorySyncCallbackLifecycle
) => Promise<void>

/** Per-client bounded executor for the optional durable-history boundary. */
export class HistorySyncBoundaryWorker {
    private readonly limits: WaHistorySyncBoundaryLimits
    private readonly process: ProcessHistorySyncWork
    private readonly onFailure: (
        reason: 'queue_full' | 'processing_failed' | 'timeout' | 'cancelled' | 'faulted'
    ) => void
    private readonly onFault: (reason: WaHistorySyncBoundaryFaultReason) => void
    private readonly queue: QueuedHistoryWork[] = []
    private readonly active = new Map<ActiveHistoryWork, true>()
    private queuedBytes = 0
    private readonly drainWaiters: Array<() => void> = []
    private status: Exclude<WaHistorySyncBoundaryState, 'DISABLED'> = 'AVAILABLE'

    public constructor(
        limits: WaHistorySyncBoundaryLimits,
        process: ProcessHistorySyncWork,
        onFailure: (
            reason: 'queue_full' | 'processing_failed' | 'timeout' | 'cancelled' | 'faulted'
        ) => void,
        onFault: (reason: WaHistorySyncBoundaryFaultReason) => void = () => undefined
    ) {
        this.limits = limits
        this.process = process
        this.onFailure = onFailure
        this.onFault = onFault
    }

    /** Synchronous admission only; payload work always starts in a later microtask. */
    public enqueue(
        notification: Proto.Message.IHistorySyncNotification,
        receipt: WaDeferredHistoryReceipt,
        onSettled: () => void,
        hasUnsupportedEnvelopeUnknowns = false
    ): boolean {
        if (this.status === 'FAULTED') {
            receipt.fail()
            onSettled()
            this.onFailure('faulted')
            return false
        }

        if (hasUnsupportedEnvelopeUnknowns || hasUnsupportedUnknownFields(notification)) {
            receipt.fail()
            onSettled()
            this.onFailure('processing_failed')
            return false
        }

        const retainedBytes = estimateRetainedBytes(notification)
        const sourceBytes = readDeclaredLength(notification)
        if (
            retainedBytes === null ||
            sourceBytes === null ||
            // maxBatchBytes is the per-job retained/source ceiling and applies
            // before either active or queued admission. maxQueuedBytes below is
            // a separate aggregate ceiling for queued jobs only.
            retainedBytes > this.limits.maxBatchBytes ||
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

    /**
     * Cancels queued/active work and waits for library-owned processing to unwind.
     * Non-cooperative application callback promises may outlive this finite drain.
     */
    public async cancelAndDrain(
        reason: unknown = new Error('history worker stopped')
    ): Promise<void> {
        if ([...this.active.keys()].some((work) => work.callbackPending)) {
            this.enterFault('callback_cancelled')
        }
        while (this.queue.length > 0) {
            const work = this.queue.shift()!
            this.queuedBytes -= work.retainedBytes
            work.receipt.fail()
            work.onSettled()
            this.onFailure('cancelled')
        }
        for (const work of this.active.keys()) {
            work.receipt.fail()
            if (!work.controller.signal.aborted) {
                work.controller.abort(reason)
                this.onFailure('cancelled')
            }
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

    public getStatus(): Exclude<WaHistorySyncBoundaryState, 'DISABLED'> {
        return this.status
    }

    private start(work: QueuedHistoryWork): void {
        const activeWork: ActiveHistoryWork = {
            ...work,
            controller: new AbortController(),
            deadlineAt: performance.now() + this.limits.timeoutMs,
            timeout: null,
            callbackStarted: false,
            callbackPending: false,
            timedOut: false
        }
        this.active.set(activeWork, true)
        activeWork.timeout = setTimeout(() => {
            this.timeoutWork(activeWork)
        }, this.limits.timeoutMs)
        activeWork.timeout.unref?.()
        queueMicrotask(() => {
            void this.run(activeWork)
        })
    }

    private async run(work: ActiveHistoryWork): Promise<void> {
        const callbackLifecycle = this.createCallbackLifecycle(work)
        try {
            await this.process(
                work.notification,
                work.controller.signal,
                work.receipt,
                callbackLifecycle
            )
        } catch {
            work.receipt.fail()
            if (!work.controller.signal.aborted) {
                this.onFailure('processing_failed')
            }
        } finally {
            // A non-cooperative callback may still retain the lifecycle object.
            // Detach it from the job so its late settlement cannot retain the
            // notification, receipt, or worker accounting record.
            callbackLifecycle.detach()
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
        while (
            this.status === 'AVAILABLE' &&
            this.active.size < this.limits.maxConcurrentBatches &&
            this.queue.length > 0
        ) {
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

    private createCallbackLifecycle(
        work: ActiveHistoryWork
    ): HistorySyncCallbackLifecycle & { detach(): void } {
        let owner: HistorySyncBoundaryWorker | null = this
        let trackedWork: ActiveHistoryWork | null = work
        return {
            onStart: () => {
                const lifecycleOwner = owner
                const activeWork = trackedWork
                return lifecycleOwner && activeWork
                    ? lifecycleOwner.startCallback(activeWork)
                    : false
            },
            onSettled: () => {
                if (!trackedWork) return
                // A v2 job invokes sequential callbacks. Retain only this one job's
                // lifecycle until run.finally detaches it, never completed payloads.
                owner?.settleCallback(trackedWork)
            },
            detach: () => {
                trackedWork = null
                owner = null
            }
        }
    }

    private startCallback(work: ActiveHistoryWork): boolean {
        if (
            this.status === 'FAULTED' ||
            work.controller.signal.aborted ||
            work.callbackPending ||
            !this.active.has(work)
        ) {
            return false
        }
        if (performance.now() >= work.deadlineAt) {
            this.timeoutWork(work)
            return false
        }
        work.callbackStarted = true
        work.callbackPending = true
        return true
    }

    private settleCallback(work: ActiveHistoryWork): void {
        if (!work.callbackPending) return
        work.callbackPending = false
        // A synchronous callback can block the event loop past its timer. Enforce
        // the same deadline at the first instruction after it settles.
        if (work.callbackStarted && performance.now() >= work.deadlineAt && !work.timedOut) {
            this.timeoutWork(work)
            this.enterFault('callback_timeout')
        }
    }

    private timeoutWork(work: ActiveHistoryWork): void {
        if (work.timedOut) return
        work.timedOut = true
        work.receipt.fail()
        work.controller.abort(new Error('history boundary processing timed out'))
        if (work.callbackPending) {
            this.enterFault('callback_timeout')
        }
        this.onFailure('timeout')
    }

    private enterFault(reason: WaHistorySyncBoundaryFaultReason): void {
        if (this.status === 'FAULTED') return
        this.status = 'FAULTED'

        let rejectedQueuedCount = 0
        while (this.queue.length > 0) {
            const work = this.queue.shift()!
            this.queuedBytes -= work.retainedBytes
            work.receipt.fail()
            work.onSettled()
            rejectedQueuedCount += 1
        }

        let cancelledActiveCount = 0
        for (const work of this.active.keys()) {
            work.receipt.fail()
            if (!work.controller.signal.aborted) {
                work.controller.abort(new Error('history boundary is faulted'))
                cancelledActiveCount += 1
            }
        }

        this.onFault(reason)
        for (let index = 0; index < rejectedQueuedCount; index += 1) {
            this.onFailure('faulted')
        }
        for (let index = 0; index < cancelledActiveCount; index += 1) {
            this.onFailure('cancelled')
        }
    }
}

function estimateRetainedBytes(
    notification: Proto.Message.IHistorySyncNotification
): number | null {
    let size = 256
    const countedBuffers = new Set<ArrayBufferLike>()
    const add = (value: Uint8Array | string | null | undefined): void => {
        if (value instanceof Uint8Array) {
            // protobufjs exposes decoded bytes as subarray views of the input
            // buffer. The worker therefore retains the backing allocation, not
            // only the visible range; count each source allocation once.
            if (!countedBuffers.has(value.buffer)) {
                countedBuffers.add(value.buffer)
                size += value.buffer.byteLength
            }
        } else if (typeof value === 'string') {
            size += value.length * 2
        }
    }
    add(notification.initialHistBootstrapInlinePayload)
    add(notification.directPath)
    add(notification.originalMessageId)
    add(notification.peerDataRequestSessionId)
    add(notification.encHandle)
    add(notification.mediaKey)
    add(notification.fileSha256)
    add(notification.fileEncSha256)
    const onDemandMetadata = notification.fullHistorySyncOnDemandRequestMetadata
    add(onDemandMetadata?.requestId)
    add(onDemandMetadata?.businessProduct)
    add(onDemandMetadata?.opaqueClientData)
    return Number.isSafeInteger(size) ? size : null
}

type ProtoMessageWithUnknownFields =
    | { readonly $unknowns?: readonly Uint8Array[] | null }
    | null
    | undefined

function hasUnknownFields(message: ProtoMessageWithUnknownFields): boolean {
    return Boolean(message?.$unknowns?.length)
}

function hasUnsupportedUnknownFields(
    notification: Proto.Message.IHistorySyncNotification
): boolean {
    return (
        hasUnknownFields(notification) ||
        hasUnknownFields(notification.fullHistorySyncOnDemandRequestMetadata) ||
        hasUnknownFields(notification.messageAccessStatus)
    )
}

function readDeclaredLength(notification: Proto.Message.IHistorySyncNotification): number | null {
    const value = notification.fileLength
    if (value === null || value === undefined) {
        return notification.initialHistBootstrapInlinePayload ? 0 : 0
    }
    const parsed = Number(typeof value === 'number' ? value : value.toString())
    return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null
}
