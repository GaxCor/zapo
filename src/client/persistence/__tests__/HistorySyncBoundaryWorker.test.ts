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

test('unknown notification bytes fail before queue admission and keep receipts withheld', async () => {
    const unknownField = new Uint8Array(1_048_576 + 5)
    // Protobuf field 16, length-delimited, with a 1 MiB payload.
    unknownField.set([0x82, 0x01, 0x80, 0x80, 0x40])
    const historyMessage: Proto.IMessage = {
        protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            historySyncNotification: {
                syncType: proto.Message.HistorySyncType.RECENT,
                fileLength: 1,
                chunkOrder: 2,
                $unknowns: [unknownField]
            }
        }
    }
    const encoded = proto.Message.encode(historyMessage).finish()
    const plaintext = new Uint8Array(encoded.byteLength + 1)
    plaintext.set(encoded)
    plaintext[encoded.byteLength] = 1

    const activeStarted = deferred()
    const activeReceipt = receipt()
    const sent: string[] = []
    const failures: string[] = []
    const settled: string[] = []
    let onBatchCalls = 0
    let historyReceipt: WaDeferredHistoryReceipt | undefined
    const worker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 1,
            maxQueuedBatches: 1,
            maxQueuedBytes: 512,
            maxBatchBytes: 2 * 1_048_576
        },
        async (input, signal) => {
            if (input.chunkOrder === 1) {
                await new Promise<void>((resolve, reject) => {
                    signal.addEventListener('abort', () => reject(signal.reason), { once: true })
                    activeStarted.resolve()
                })
                return
            }
            onBatchCalls += 1
        },
        (reason) => failures.push(reason)
    )

    assert.equal(
        worker.enqueue({ ...notification(), chunkOrder: 1 }, activeReceipt.value, () => undefined),
        true
    )
    await activeStarted.promise

    const handled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'history-unknowns', from: '551100000000@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            sendNode: async (node) => {
                sent.push(node.tag + ':' + (node.attrs.type ?? 'delivery'))
            },
            captureBoundSendNode: () => async (node) => {
                sent.push(node.tag + ':' + (node.attrs.type ?? 'delivery'))
            },
            signalProtocol: { decryptMessage: async () => plaintext } as never,
            scheduleHistorySync: (event, receiptValue) => {
                historyReceipt = receiptValue
                const input = event.message?.protocolMessage?.historySyncNotification
                assert.ok(input)
                return worker.enqueue(input, receiptValue, () => settled.push('unknown'))
            }
        }
    )

    assert.equal(handled, true)
    assert.equal(worker.running(), 1)
    assert.equal(worker.queued(), 0)
    assert.equal(onBatchCalls, 0)
    assert.deepEqual(settled, ['unknown'])
    assert.deepEqual(failures, ['processing_failed'])
    assert.deepEqual(sent, [])
    assert.ok(historyReceipt)
    await historyReceipt.sendHistorySyncReceipt()
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.deepEqual(sent, [], 'rejected history work must suppress both receipt kinds')

    // A following bounded item still fits, proving rejection retained neither
    // a queue slot nor accounted bytes from the oversized unknown payload.
    const followingReceipt = receipt()
    assert.equal(
        worker.enqueue(
            { ...notification(), chunkOrder: 3 },
            followingReceipt.value,
            () => undefined
        ),
        true
    )
    assert.equal(worker.running(), 1)
    assert.equal(worker.queued(), 1)
    assert.equal(onBatchCalls, 0)

    await worker.cancelAndDrain()
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
    assert.equal(onBatchCalls, 0)
})

test('history worker rejects nested unknown fields before retention', () => {
    const nestedUnknownNotifications: Proto.Message.IHistorySyncNotification[] = [
        {
            ...notification(),
            fullHistorySyncOnDemandRequestMetadata: {
                $unknowns: [new Uint8Array([0x08, 0x01])]
            }
        },
        {
            ...notification(),
            messageAccessStatus: { $unknowns: [new Uint8Array([0x08, 0x01])] }
        }
    ]

    for (const input of nestedUnknownNotifications) {
        const itemReceipt = receipt()
        const failures: string[] = []
        const worker = new HistorySyncBoundaryWorker(
            { ...LIMITS, maxConcurrentBatches: 0, maxQueuedBatches: 1, maxQueuedBytes: 512 },
            async () => assert.fail('unsupported nested unknown fields must not be processed'),
            (reason) => failures.push(reason)
        )

        assert.equal(
            worker.enqueue(input, itemReceipt.value, () => undefined),
            false
        )
        assert.equal(worker.running(), 0)
        assert.equal(worker.queued(), 0)
        assert.deepEqual(itemReceipt.state, ['fail'])
        assert.deepEqual(failures, ['processing_failed'])
    }
})

test('history worker accounts for nested opaque client bytes before queue admission', () => {
    const itemReceipt = receipt()
    const failures: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxConcurrentBatches: 0, maxQueuedBatches: 1, maxQueuedBytes: 512 },
        async () => assert.fail('over-budget nested bytes must not be processed'),
        (reason) => failures.push(reason)
    )

    assert.equal(
        worker.enqueue(
            {
                ...notification(),
                fullHistorySyncOnDemandRequestMetadata: {
                    opaqueClientData: new Uint8Array(257)
                }
            },
            itemReceipt.value,
            () => undefined
        ),
        false
    )
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.deepEqual(failures, ['queue_full'])
})

test('history worker accounts for a retained protobuf byte view backing buffer', () => {
    const backing = new Uint8Array(4_096)
    const itemReceipt = receipt()
    const failures: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 0,
            maxQueuedBatches: 1,
            maxQueuedBytes: 512,
            maxBatchBytes: 8_192
        },
        async () => assert.fail('over-budget backing allocation must not be processed'),
        (reason) => failures.push(reason)
    )

    assert.equal(
        worker.enqueue(
            { ...notification(), mediaKey: backing.subarray(0, 1) },
            itemReceipt.value,
            () => undefined
        ),
        false
    )
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.deepEqual(failures, ['queue_full'])
})
