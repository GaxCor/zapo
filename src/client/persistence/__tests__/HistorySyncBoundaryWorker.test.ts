import assert from 'node:assert/strict'
import test from 'node:test'

import { HistorySyncBoundaryWorker } from '@client/persistence/HistorySyncBoundaryWorker'
import { createNoopLogger } from '@infra/log/types'
import {
    handleIncomingMessageAck,
    type WaDeferredHistoryReceipt
} from '@message/primitives/incoming'
import { proto, type Proto } from '@proto'

const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 2_048,
    maxBatchBytes: 1_024,
    maxMessagesPerBatch: 8,
    maxSourceRecordsPerBatch: 64,
    timeoutMs: 1_000
} as const

function notification(): Proto.Message.IHistorySyncNotification {
    return { syncType: 3, fileLength: 1 }
}

function receipt(): { readonly value: WaDeferredHistoryReceipt; readonly state: string[] } {
    const state: string[] = []
    let settled = false
    return {
        state,
        value: {
            complete: () => {
                if (settled) return
                settled = true
                state.push('complete')
            },
            fail: () => {
                if (settled) return
                settled = true
                state.push('fail')
            },
            sendHistorySyncReceipt: async () => {
                state.push('hist_sync')
            }
        }
    }
}

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
    let resolve!: () => void
    const promise = new Promise<void>((done) => {
        resolve = done
    })
    return { promise, resolve }
}

test('history worker bounds jobs while the normal incoming handler completes beside a blocked callback', async () => {
    const started: string[] = []
    const firstBarrier = deferred()
    const secondBarrier = deferred()
    const receipts = [receipt(), receipt(), receipt()]
    const settled: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        async (_input, _signal, current) => {
            const index = started.length
            started.push(`history:${index}`)
            await (index === 0 ? firstBarrier.promise : secondBarrier.promise)
            current.complete()
        },
        (reason) => {
            assert.equal(reason, 'queue_full')
        }
    )

    assert.equal(
        worker.enqueue(notification(), receipts[0].value, () => settled.push('one')),
        true
    )
    assert.equal(
        worker.enqueue(notification(), receipts[1].value, () => settled.push('two')),
        true
    )
    assert.equal(
        worker.enqueue(notification(), receipts[2].value, () => settled.push('three')),
        false
    )
    await Promise.resolve()
    assert.deepEqual(started, ['history:0'])
    assert.equal(worker.running(), 1)
    assert.equal(worker.queued(), 1)
    assert.deepEqual(receipts[2].state, ['fail'])

    // Exercise the actual incoming stanza handler while the history worker is
    // parked at a promise barrier. It uses no history gate for an ordinary text.
    const livePath: string[] = []
    const liveHandled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'live-1', from: '551100000000@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            sendNode: async (node) => {
                livePath.push(`receipt:${node.tag}`)
            },
            signalProtocol: {
                decryptMessage: async () => {
                    const bytes = proto.Message.encode({ conversation: 'live' }).finish()
                    const padded = new Uint8Array(bytes.length + 1)
                    padded.set(bytes)
                    padded[bytes.length] = 1
                    return padded
                }
            } as never,
            emitIncomingMessage: () => {
                livePath.push('live:processed')
            }
        }
    )
    assert.equal(liveHandled, true)
    assert.deepEqual(livePath, ['live:processed', 'receipt:receipt'])
    assert.deepEqual(started, ['history:0'])

    firstBarrier.resolve()
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.deepEqual(started, ['history:0', 'history:1'])
    assert.deepEqual(receipts[0].state, ['complete'])
    assert.deepEqual(receipts[1].state, [])

    secondBarrier.resolve()
    await new Promise<void>((resolve) => setImmediate(resolve))
    await worker.cancelAndDrain()
    assert.deepEqual(receipts[1].state, ['complete'])
    assert.deepEqual(settled, ['three', 'one', 'two'])
    assert.equal(worker.running(), 0)
})

test('history worker cancellation fails queued and active receipts and signals active callback', async () => {
    const started = deferred()
    const receipts = [receipt(), receipt()]
    let observedAbort = false
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        async (_input, signal) => {
            started.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener(
                    'abort',
                    () => {
                        observedAbort = true
                        reject(signal.reason)
                    },
                    { once: true }
                )
            })
        },
        () => undefined
    )
    worker.enqueue(notification(), receipts[0].value, () => undefined)
    worker.enqueue(notification(), receipts[1].value, () => undefined)
    await started.promise
    await worker.cancelAndDrain()
    assert.equal(observedAbort, true)
    assert.deepEqual(receipts[0].state, ['fail'])
    assert.deepEqual(receipts[1].state, ['fail'])
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
})

test('history worker timeout aborts a never-resolving callback and never completes receipt', async () => {
    const started = deferred()
    const timedOut = deferred()
    const settled = deferred()
    const keepAlive = setTimeout(() => undefined, 1_000)
    const itemReceipt = receipt()
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxQueuedBatches: 0, maxQueuedBytes: 0, timeoutMs: 15 },
        async (_input, signal) => {
            started.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        (reason) => {
            assert.equal(reason, 'timeout')
            timedOut.resolve()
        }
    )
    worker.enqueue(notification(), itemReceipt.value, () => settled.resolve())
    await started.promise
    await timedOut.promise
    await settled.promise
    clearTimeout(keepAlive)
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.equal(worker.running(), 0)
})

test('history worker callback rejection fails closed without receipt completion', async () => {
    const failed = deferred()
    const settled = deferred()
    const itemReceipt = receipt()
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxQueuedBatches: 0, maxQueuedBytes: 0 },
        async () => {
            throw new Error('private callback details must not escape')
        },
        (reason) => {
            assert.equal(reason, 'processing_failed')
            failed.resolve()
        }
    )
    worker.enqueue(notification(), itemReceipt.value, () => settled.resolve())
    await failed.promise
    await settled.promise
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.equal(worker.running(), 0)
})

test('history worker rejects oversized and over-capacity jobs without admission', () => {
    const failures: string[] = []
    const first = receipt()
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxConcurrentBatches: 1, maxQueuedBatches: 0, maxQueuedBytes: 0 },
        async () => new Promise<void>(() => undefined),
        (reason) => failures.push(reason)
    )
    assert.equal(
        worker.enqueue({ ...notification(), fileLength: 2_048 }, first.value, () => undefined),
        false
    )
    assert.deepEqual(first.state, ['fail'])
    assert.deepEqual(failures, ['processing_failed'])
})
