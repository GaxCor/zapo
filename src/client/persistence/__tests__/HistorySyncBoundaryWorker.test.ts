import assert from 'node:assert/strict'
import { createServer, type Server } from 'node:http'
import type { Readable } from 'node:stream'
import test from 'node:test'
import { gzipSync } from 'node:zlib'

import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { HistorySyncBoundaryWorker } from '@client/persistence/HistorySyncBoundaryWorker'
import { createNoopLogger } from '@infra/log/types'
import { WaMediaTransferClient } from '@media/transfer/WaMediaTransferClient'
import {
    handleIncomingMessageAck,
    type WaDeferredHistoryReceipt
} from '@message/primitives/incoming'
import { proto, type Proto } from '@proto'
import type { RawWebSocketConstructor } from '@transport/types'
import { WaComms } from '@transport/WaComms'

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

function createCommsForGenerationTest(): WaComms {
    const socketConstructor = class {
        public binaryType = 'arraybuffer'
        public readyState = 0
        public onopen: unknown = null
        public onclose: unknown = null
        public onerror: unknown = null
        public onmessage: unknown = null
        public close(): void {}
        public send(): void {}
    } as unknown as RawWebSocketConstructor
    return new WaComms(
        {
            urls: ['wss://example.invalid/ws'],
            rawWebSocketConstructor: socketConstructor,
            noise: {
                clientStaticKeyPair: {
                    pubKey: new Uint8Array(32),
                    privKey: new Uint8Array(32)
                },
                isRegistered: false
            }
        },
        createNoopLogger()
    )
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

function wireVarint(value: number): Uint8Array {
    const bytes: number[] = []
    let remaining = value
    while (remaining > 0x7f) {
        bytes.push((remaining & 0x7f) | 0x80)
        remaining = Math.floor(remaining / 128)
    }
    bytes.push(remaining)
    return Uint8Array.from(bytes)
}

function wireConcat(...parts: readonly Uint8Array[]): Uint8Array {
    const result = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0))
    let offset = 0
    for (const part of parts) {
        result.set(part, offset)
        offset += part.byteLength
    }
    return result
}

function wireBytes(fieldNumber: number, value: Uint8Array): Uint8Array {
    return wireConcat(wireVarint(fieldNumber * 8 + 2), wireVarint(value.byteLength), value)
}

function wireString(fieldNumber: number, value: string): Uint8Array {
    return wireBytes(fieldNumber, new TextEncoder().encode(value))
}

function wireInteger(fieldNumber: number, value: number): Uint8Array {
    return wireConcat(wireVarint(fieldNumber * 8), wireVarint(value))
}

function historyBytesWithExtendedTextUnknown(): Uint8Array {
    const address = '5511000000001@s.whatsapp.net'
    const extendedText = wireConcat(wireString(1, 'visible text'), wireInteger(99, 1))
    const message = wireBytes(6, extendedText)
    const key = wireConcat(wireString(1, address), wireInteger(2, 0), wireString(3, 'msg-1'))
    const webMessage = wireConcat(
        wireBytes(1, key),
        wireBytes(2, message),
        wireInteger(3, 1_700_000_000)
    )
    const historyMessage = wireBytes(1, webMessage)
    const conversation = wireConcat(wireString(1, address), wireBytes(2, historyMessage))
    return wireConcat(
        wireInteger(1, proto.HistorySync.HistorySyncType.RECENT),
        wireBytes(2, conversation)
    )
}

async function listen(server: Server): Promise<string> {
    await new Promise<void>((resolve, reject) => {
        server.once('error', reject)
        server.listen(0, '127.0.0.1', resolve)
    })
    const address = server.address()
    assert.ok(address && typeof address !== 'string')
    return `http://127.0.0.1:${address.port}/history`
}

async function close(server: Server): Promise<void> {
    if (!server.listening) return
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()))
    })
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
    const releaseLateCallback = deferred()
    const lateCallbackSettled = deferred()
    const faulted = deferred()
    const keepAlive = setTimeout(() => undefined, 1_000)
    const itemReceipt = receipt()
    let callbackPending = false
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxQueuedBatches: 0, maxQueuedBytes: 0, timeoutMs: 15 },
        async (_input, signal, currentReceipt, callbackLifecycle) => {
            assert.equal(callbackLifecycle.onStart(), true)
            callbackPending = true
            void releaseLateCallback.promise.then(() => {
                callbackPending = false
                callbackLifecycle.onSettled()
                currentReceipt.complete()
                lateCallbackSettled.resolve()
            })
            started.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        (reason) => {
            assert.equal(reason, 'timeout')
            timedOut.resolve()
        },
        (reason) => {
            assert.equal(reason, 'callback_timeout')
            faulted.resolve()
        }
    )
    worker.enqueue(notification(), itemReceipt.value, () => settled.resolve())
    await started.promise
    await timedOut.promise
    await faulted.promise
    await settled.promise
    clearTimeout(keepAlive)
    assert.equal(callbackPending, true, 'worker must not pretend callback code terminated')
    assert.equal(worker.getStatus(), 'FAULTED')
    assert.equal(worker.running(), 0, 'the abortable job settles without waiting for callback code')
    releaseLateCallback.resolve()
    await lateCallbackSettled.promise
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.equal(worker.getStatus(), 'FAULTED', 'late callback completion cannot clear the fault')
})

test('faulted history worker rejects future jobs while live direct-message receipts continue', async () => {
    const callback = deferred()
    const callbackStarted = deferred()
    const faulted = deferred()
    const firstReceipt = receipt()
    const laterReceipt = receipt()
    const keepAlive = setTimeout(() => undefined, 1_000)
    let callbackPending = false
    let callbackStarts = 0
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxConcurrentBatches: 1, maxQueuedBatches: 1, timeoutMs: 15 },
        async (_input, signal, _currentReceipt, lifecycle) => {
            if (!lifecycle.onStart()) throw new Error('callback admission unexpectedly closed')
            callbackStarts += 1
            callbackPending = true
            void callback.promise.then(() => {
                callbackPending = false
                lifecycle.onSettled()
            })
            callbackStarted.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        () => undefined,
        (reason) => {
            assert.equal(reason, 'callback_timeout')
            faulted.resolve()
        }
    )
    worker.enqueue(notification(), firstReceipt.value, () => undefined)
    await callbackStarted.promise
    await faulted.promise
    clearTimeout(keepAlive)
    await worker.cancelAndDrain()
    assert.equal(callbackPending, true)
    assert.equal(worker.getStatus(), 'FAULTED')
    assert.equal(worker.running(), 0)

    assert.equal(
        worker.enqueue(notification(), laterReceipt.value, () => undefined),
        false
    )
    assert.deepEqual(laterReceipt.state, ['fail'])
    assert.equal(callbackStarts, 1, 'no post-fault callback may start')

    // A real WaComms generation change models Noise-session replacement on
    // reconnect; it cannot recreate this per-client worker or clear its fault.
    const comms = createCommsForGenerationTest()
    const replaceSession = (
        comms as unknown as {
            replaceNoiseSession(session: unknown): void
        }
    ).replaceNoiseSession.bind(comms)
    replaceSession({ encryptFrame: async () => new Uint8Array([1]) })
    const oldGeneration = comms.getConnectionGeneration()
    replaceSession({ encryptFrame: async () => new Uint8Array([2]) })
    assert.notEqual(comms.getConnectionGeneration(), oldGeneration)
    assert.equal(worker.getStatus(), 'FAULTED')

    const livePath: string[] = []
    const liveHandled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'live-after-history-fault', from: '551100000000@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            sendNode: async () => {
                livePath.push('receipt')
            },
            signalProtocol: {
                decryptMessage: async () => {
                    const bytes = proto.Message.encode({ conversation: 'still live' }).finish()
                    const padded = new Uint8Array(bytes.length + 1)
                    padded.set(bytes)
                    padded[bytes.length] = 1
                    return padded
                }
            } as never,
            emitIncomingMessage: () => livePath.push('message')
        }
    )
    assert.equal(liveHandled, true)
    assert.deepEqual(livePath, ['message', 'receipt'])

    callback.resolve()
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.deepEqual(firstReceipt.state, ['fail'])
    assert.equal(worker.getStatus(), 'FAULTED')
})

test('late non-cooperative callback rejection is observed after finite shutdown', async () => {
    let rejectCallback!: (error: Error) => void
    const appCallback = new Promise<void>((_resolve, reject) => {
        rejectCallback = reject
    })
    const callbackStarted = deferred()
    const callbackSettled = deferred()
    const faulted = deferred()
    const itemReceipt = receipt()
    let callbackPending = false
    let observedRejection = false
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxConcurrentBatches: 1, timeoutMs: 10_000 },
        async (_input, signal, _currentReceipt, lifecycle) => {
            assert.equal(lifecycle.onStart(), true)
            callbackPending = true
            // Attach both handlers now: a late application rejection is observed
            // without keeping a worker-owned retired-callback collection.
            void appCallback.then(
                () => {
                    callbackPending = false
                    lifecycle.onSettled()
                    callbackSettled.resolve()
                },
                () => {
                    observedRejection = true
                    callbackPending = false
                    lifecycle.onSettled()
                    callbackSettled.resolve()
                }
            )
            callbackStarted.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        () => undefined,
        (reason) => {
            assert.equal(reason, 'callback_cancelled')
            faulted.resolve()
        }
    )
    worker.enqueue(notification(), itemReceipt.value, () => undefined)
    await callbackStarted.promise
    const shutdown = worker.cancelAndDrain()
    await faulted.promise
    await shutdown
    assert.equal(worker.getStatus(), 'FAULTED')
    assert.equal(callbackPending, true)
    assert.equal(worker.running(), 0)
    assert.deepEqual(itemReceipt.state, ['fail'])

    rejectCallback(new Error('private late callback failure'))
    await callbackSettled.promise
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.equal(observedRejection, true)
    assert.equal(worker.getStatus(), 'FAULTED')
    assert.deepEqual(itemReceipt.state, ['fail'])
})

test('one timed-out callback faults all active history work and bounds unresolved callbacks by concurrency', async () => {
    const callbacks = [deferred(), deferred()]
    const bothStarted = deferred()
    const faulted = deferred()
    const receipts = [receipt(), receipt(), receipt(), receipt()]
    const keepAlive = setTimeout(() => undefined, 1_000)
    let starts = 0
    let unresolvedCallbacks = 0
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxConcurrentBatches: 2, maxQueuedBatches: 2, timeoutMs: 20 },
        async (_input, signal, _currentReceipt, lifecycle) => {
            const index = starts++
            assert.equal(lifecycle.onStart(), true)
            unresolvedCallbacks += 1
            void callbacks[index].promise.then(() => {
                unresolvedCallbacks -= 1
                lifecycle.onSettled()
            })
            if (starts === 2) bothStarted.resolve()
            await new Promise<void>((_resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        () => undefined,
        (reason) => {
            assert.equal(reason, 'callback_timeout')
            faulted.resolve()
        }
    )
    worker.enqueue(notification(), receipts[0].value, () => undefined)
    worker.enqueue(notification(), receipts[1].value, () => undefined)
    worker.enqueue(notification(), receipts[2].value, () => undefined)
    await bothStarted.promise
    await faulted.promise
    clearTimeout(keepAlive)
    await worker.cancelAndDrain()
    assert.equal(worker.getStatus(), 'FAULTED')
    assert.equal(worker.running(), 0)
    assert.equal(unresolvedCallbacks, 2)
    assert.ok(
        unresolvedCallbacks <= 2,
        'unresolved callback count is bounded by pre-fault concurrency'
    )

    assert.equal(
        worker.enqueue(notification(), receipts[3].value, () => undefined),
        false
    )
    assert.equal(starts, 2)
    assert.deepEqual(
        receipts.map((entry) => entry.state),
        [['fail'], ['fail'], ['fail'], ['fail']]
    )

    callbacks[0].resolve()
    callbacks[1].resolve()
    await new Promise<void>((resolve) => setImmediate(resolve))
    assert.equal(unresolvedCallbacks, 0)
    assert.equal(worker.getStatus(), 'FAULTED')
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

test('history worker applies the per-job retained-byte bound before active or queued admission', async () => {
    const oversize = { ...notification(), directPath: 'x'.repeat(4_096) }
    const activeReceipt = receipt()
    const activeStarted = deferred()
    const failures: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 1,
            maxQueuedBatches: 1,
            maxQueuedBytes: 8_192,
            maxBatchBytes: 512
        },
        async (_input, signal) => {
            activeStarted.resolve()
            await new Promise<void>((resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        (reason) => failures.push(reason)
    )

    assert.equal(
        worker.enqueue(notification(), activeReceipt.value, () => undefined),
        true
    )
    await activeStarted.promise

    const occupiedSlotReceipt = receipt()
    assert.equal(
        worker.enqueue(oversize, occupiedSlotReceipt.value, () => undefined),
        false,
        'an occupied active slot must not turn an individually oversized item into a queue candidate'
    )
    assert.equal(worker.running(), 1)
    assert.equal(worker.queued(), 0)
    assert.deepEqual(occupiedSlotReceipt.state, ['fail'])

    const immediateWorker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 1,
            maxQueuedBatches: 0,
            maxQueuedBytes: 0,
            maxBatchBytes: 512
        },
        async () => assert.fail('oversized immediately-active work must not start'),
        (reason) => failures.push(reason)
    )
    const immediateReceipt = receipt()
    assert.equal(
        immediateWorker.enqueue(oversize, immediateReceipt.value, () => undefined),
        false
    )
    assert.equal(immediateWorker.running(), 0)
    assert.equal(immediateWorker.queued(), 0)
    assert.deepEqual(immediateReceipt.state, ['fail'])
    assert.deepEqual(failures, ['processing_failed', 'processing_failed'])

    await worker.cancelAndDrain()
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
})

test('history worker accepts the exact per-job byte boundary and enforces aggregate queued bytes separately', async () => {
    const exactReceipt = receipt()
    const exactSettled = deferred()
    const exactWorker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 1,
            maxQueuedBatches: 0,
            maxQueuedBytes: 0,
            maxBatchBytes: 512
        },
        async (_input, currentSignal, currentReceipt) => {
            assert.equal(currentSignal.aborted, false)
            currentReceipt.complete()
        },
        () => assert.fail('the exact retained-byte boundary must be admitted')
    )
    const exactBoundary = { ...notification(), directPath: 'x'.repeat(128) }
    assert.equal(
        exactWorker.enqueue(exactBoundary, exactReceipt.value, () => exactSettled.resolve()),
        true
    )
    await exactSettled.promise
    assert.deepEqual(exactReceipt.state, ['complete'])

    const activeStarted = deferred()
    const queuedFailures: string[] = []
    const aggregateWorker = new HistorySyncBoundaryWorker(
        {
            ...LIMITS,
            maxConcurrentBatches: 1,
            maxQueuedBatches: 2,
            maxQueuedBytes: 512,
            maxBatchBytes: 512
        },
        async (_input, signal) => {
            activeStarted.resolve()
            await new Promise<void>((resolve, reject) => {
                signal.addEventListener('abort', () => reject(signal.reason), { once: true })
            })
        },
        (reason) => queuedFailures.push(reason)
    )
    const receipts = [receipt(), receipt(), receipt()]
    assert.equal(
        aggregateWorker.enqueue(notification(), receipts[0].value, () => undefined),
        true
    )
    await activeStarted.promise
    assert.equal(
        aggregateWorker.enqueue(exactBoundary, receipts[1].value, () => undefined),
        true,
        'one 512-byte retained job fits the aggregate queued-byte budget exactly'
    )
    assert.equal(aggregateWorker.queued(), 1)
    assert.equal(
        aggregateWorker.enqueue(notification(), receipts[2].value, () => undefined),
        false,
        'the aggregate queued-byte budget is distinct from the per-job bound'
    )
    assert.equal(aggregateWorker.running(), 1)
    assert.equal(aggregateWorker.queued(), 1)
    assert.deepEqual(receipts[2].state, ['fail'])
    assert.deepEqual(queuedFailures, ['queue_full'])
    await aggregateWorker.cancelAndDrain()
})

test('history worker rejects unknowns on consumed notification envelope wrappers', () => {
    const itemReceipt = receipt()
    const failures: string[] = []
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        async () => assert.fail('unsupported wrapper unknowns must not be processed'),
        (reason) => failures.push(reason)
    )
    assert.equal(
        worker.enqueue(notification(), itemReceipt.value, () => undefined, true),
        false
    )
    assert.deepEqual(itemReceipt.state, ['fail'])
    assert.deepEqual(failures, ['processing_failed'])
    assert.equal(worker.running(), 0)
    assert.equal(worker.queued(), 0)
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

test('visible ExtendedTextMessage unknowns fail closed through the receipt gate', async () => {
    const payload = historyBytesWithExtendedTextUnknown()
    const decoded = proto.HistorySync.decode(payload)
    assert.equal(
        decoded.conversations?.[0]?.messages?.[0]?.message?.message?.extendedTextMessage?.$unknowns
            ?.length,
        1
    )
    const notificationValue: Proto.Message.IHistorySyncNotification = {
        syncType: proto.Message.HistorySyncType.RECENT,
        fileLength: 1,
        initialHistBootstrapInlinePayload: gzipSync(payload)
    }
    const incomingMessage: Proto.IMessage = {
        protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            historySyncNotification: notificationValue
        }
    }
    const encoded = proto.Message.encode(incomingMessage).finish()
    const padded = new Uint8Array(encoded.byteLength + 1)
    padded.set(encoded)
    padded[padded.byteLength - 1] = 1

    const failures: string[] = []
    const settled = deferred()
    const sent: string[] = []
    let callbackCalled = false
    let historyReceipt: WaDeferredHistoryReceipt | undefined
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxBatchBytes: 16_384, maxQueuedBytes: 16_384 },
        async (input, signal, currentReceipt) =>
            processHistorySyncNotification(
                {
                    logger: createNoopLogger(),
                    mediaTransfer: {} as never,
                    writeBehind: {
                        ownHistoryOperation: (start: () => Promise<void>) => start(),
                        historyOperationsSettled: () => false,
                        persistMessageAsync: async () => undefined,
                        persistThreadAsync: async () => undefined,
                        persistContactAsync: async () => undefined
                    } as never,
                    emitEvent: () => undefined,
                    onProcessed: async () => assert.fail('failed batch must not be acknowledged')
                } as never,
                input,
                {
                    limits: {
                        ...LIMITS,
                        maxBatchBytes: 16_384,
                        maxQueuedBytes: 16_384
                    },
                    signal,
                    onBatch: async () => {
                        callbackCalled = true
                        currentReceipt.complete()
                    }
                }
            ),
        (reason) => failures.push(reason)
    )
    const handled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'history-extended-text-unknown', from: '5511000000001@s.whatsapp.net' },
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
            signalProtocol: { decryptMessage: async () => padded } as never,
            scheduleHistorySync: (event, receiptValue) => {
                historyReceipt = receiptValue
                const input = event.message?.protocolMessage?.historySyncNotification
                assert.ok(input)
                return worker.enqueue(input, receiptValue, () => settled.resolve())
            }
        }
    )

    assert.equal(handled, true)
    await settled.promise
    assert.equal(callbackCalled, false)
    assert.deepEqual(failures, ['processing_failed'])
    assert.equal(JSON.stringify(failures).includes('visible text'), false)
    assert.ok(historyReceipt)
    await historyReceipt.sendHistorySyncReceipt()
    assert.deepEqual(sent, [], 'neither ordinary nor hist_sync receipt may escape')
})

test('reconnect after history decode permanently stales the deferred receipt sender', async () => {
    const historyPayload = proto.HistorySync.encode({
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        conversations: []
    }).finish()
    const notificationValue: Proto.Message.IHistorySyncNotification = {
        syncType: proto.Message.HistorySyncType.RECENT,
        fileLength: 1,
        initialHistBootstrapInlinePayload: gzipSync(historyPayload)
    }
    const message: Proto.IMessage = {
        protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            historySyncNotification: notificationValue
        }
    }
    const encoded = proto.Message.encode(message).finish()
    const padded = new Uint8Array(encoded.byteLength + 1)
    padded.set(encoded)
    padded[padded.byteLength - 1] = 1

    const callbackEntered = deferred()
    const releaseCallback = deferred()
    const jobSettled = deferred()
    const sent: string[] = []
    const failures: string[] = []
    let generation = 2
    let historyReceipt: WaDeferredHistoryReceipt | undefined
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, maxBatchBytes: 16_384, maxQueuedBytes: 16_384 },
        async (input, signal, currentReceipt) =>
            processHistorySyncNotification(
                {
                    logger: createNoopLogger(),
                    mediaTransfer: {} as never,
                    writeBehind: {
                        ownHistoryOperation: (start: () => Promise<void>) => start(),
                        historyOperationsSettled: () => false,
                        persistMessageAsync: async () => undefined,
                        persistThreadAsync: async () => undefined,
                        persistContactAsync: async () => undefined
                    } as never,
                    emitEvent: () => undefined,
                    onProcessed: async () => undefined
                } as never,
                input,
                {
                    limits: {
                        ...LIMITS,
                        maxBatchBytes: 16_384,
                        maxQueuedBytes: 16_384
                    },
                    signal,
                    onBatch: async () => {
                        // onBatch is entered only after the inline history body
                        // was fully decoded and its integrity gate completed.
                        callbackEntered.resolve()
                        await releaseCallback.promise
                        currentReceipt.complete()
                    }
                }
            ),
        (reason) => failures.push(reason)
    )

    const handled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'history-reconnect-race', from: '5511000000001@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            sendNode: async (node) => {
                sent.push(node.tag + ':' + (node.attrs.type ?? 'delivery'))
            },
            captureBoundSendNode: () => {
                const capturedGeneration = generation
                return async (node) => {
                    if (capturedGeneration !== generation) {
                        throw new Error('captured connection generation is stale')
                    }
                    sent.push(node.tag + ':' + (node.attrs.type ?? 'delivery'))
                }
            },
            signalProtocol: { decryptMessage: async () => padded } as never,
            scheduleHistorySync: (event, receiptValue) => {
                historyReceipt = receiptValue
                const input = event.message?.protocolMessage?.historySyncNotification
                assert.ok(input)
                return worker.enqueue(input, receiptValue, () => jobSettled.resolve())
            }
        }
    )

    assert.equal(handled, true)
    await callbackEntered.promise
    generation += 1 // replacement connection arrives after body decode, before adapter fulfillment
    releaseCallback.resolve()
    await jobSettled.promise

    assert.deepEqual(failures, [])
    assert.ok(historyReceipt)
    await assert.rejects(historyReceipt.sendHistorySyncReceipt(), /generation is stale/)
    assert.deepEqual(
        sent,
        [],
        'late callback fulfillment must never route either receipt to the new connection'
    )
})

test('abort after headers destroys stalled history body, drains the job, and withholds receipts', async () => {
    const serverResponseClosed = deferred()
    const server = createServer((_request, response) => {
        response.once('close', serverResponseClosed.resolve)
        response.writeHead(200)
        response.flushHeaders()
    })
    const url = await listen(server)
    const bodyReady = deferred()
    const jobSettled = deferred()
    const failures: string[] = []
    const sent: string[] = []
    const bodyReference: { value: Readable | null } = { value: null }
    let historyReceipt: WaDeferredHistoryReceipt | undefined
    const mediaTransfer = new WaMediaTransferClient()
    const worker = new HistorySyncBoundaryWorker(
        { ...LIMITS, timeoutMs: 5_000 },
        async (_input, signal) => {
            const response = await mediaTransfer.downloadStream({ url, signal, timeoutMs: 5_000 })
            bodyReference.value = response.body
            assert.ok(bodyReference.value)
            bodyReady.resolve()
            for await (const chunk of bodyReference.value) {
                void chunk
            }
        },
        (reason) => failures.push(reason)
    )
    const message: Proto.IMessage = {
        protocolMessage: {
            type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
            historySyncNotification: notification()
        }
    }
    const encoded = proto.Message.encode(message).finish()
    const padded = new Uint8Array(encoded.byteLength + 1)
    padded.set(encoded)
    padded[padded.byteLength - 1] = 1

    try {
        const handled = await handleIncomingMessageAck(
            {
                tag: 'message',
                attrs: { id: 'history-stalled-body', from: '5511000000001@s.whatsapp.net' },
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
                signalProtocol: { decryptMessage: async () => padded } as never,
                scheduleHistorySync: (event, receiptValue) => {
                    historyReceipt = receiptValue
                    const input = event.message?.protocolMessage?.historySyncNotification
                    assert.ok(input)
                    return worker.enqueue(input, receiptValue, () => jobSettled.resolve())
                }
            }
        )
        assert.equal(handled, true)
        await bodyReady.promise
        const receivedBody = bodyReference.value
        assert.ok(receivedBody)
        const bodyClosed = new Promise<void>((resolve) => receivedBody.once('close', resolve))
        await worker.cancelAndDrain(new Error('test cancellation after response headers'))
        await Promise.all([bodyClosed, serverResponseClosed.promise, jobSettled.promise])

        assert.equal(receivedBody.destroyed, true)
        assert.deepEqual(failures, ['cancelled'])
        assert.ok(historyReceipt)
        await historyReceipt.sendHistorySyncReceipt()
        assert.deepEqual(sent, [], 'cancelled body transfer must not release either receipt')
    } finally {
        await close(server)
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

function acceptedDispositionsPayload(): Uint8Array {
    const quoted = wireBytes(
        6,
        wireConcat(wireString(1, 'synthetic-quote'), wireString(4, 'synthetic-url-quoted'))
    )
    const content = wireBytes(
        6,
        wireConcat(
            wireString(1, 'synthetic-direct'),
            wireString(4, 'synthetic-url-outer'),
            wireBytes(17, wireBytes(3, quoted))
        )
    )
    const key = proto.MessageKey.encode({
        remoteJid: 'synthetic-peer@s.whatsapp.net',
        id: 'synthetic-key',
        fromMe: false
    }).finish()
    const historyRecord = wireBytes(
        1,
        wireConcat(wireBytes(1, key), wireBytes(2, content), wireInteger(3, 123))
    )
    return wireConcat(
        wireInteger(1, 3),
        wireBytes(8, new Uint8Array()),
        wireBytes(9, new Uint8Array([255])),
        wireInteger(10, 1),
        wireBytes(12, wireString(1, 'synthetic-group@g.us')),
        wireBytes(13, new Uint8Array()),
        wireBytes(
            2,
            wireConcat(wireString(1, 'synthetic-peer@s.whatsapp.net'), wireBytes(2, historyRecord))
        )
    )
}

for (const mode of [
    'success',
    'reject',
    'cancel',
    'timeout',
    'stale',
    'private_failure'
] as const) {
    test(`INT-01..07 accepted mixed batch held beside live delivery; settlement ${mode}`, async () => {
        const limits = {
            ...LIMITS,
            maxBatchBytes: 65_536,
            maxQueuedBytes: 65_536,
            maxSourceRecordsPerBatch: 1_000,
            timeoutMs: mode === 'timeout' ? 50 : 2_000
        }
        const entered = deferred()
        const release = deferred()
        const settled = deferred()
        const sent: string[] = []
        const failures: string[] = []
        const logs: unknown[] = []
        const logger = {
            ...createNoopLogger(),
            info: (message: string, ctx?: unknown) => logs.push([message, ctx]),
            warn: (message: string, ctx?: unknown) => logs.push([message, ctx])
        }
        let generation = 1
        let writes = 0
        const worker = new HistorySyncBoundaryWorker(
            limits,
            async (input, signal, current, lifecycle) => {
                await processHistorySyncNotification(
                    {
                        logger,
                        mediaTransfer: {} as never,
                        writeBehind: {
                            ownHistoryOperation: (start: () => Promise<void>) => start(),
                            historyOperationsSettled: () => false,
                            persistMessageAsync: async (row: { messageBytes: Uint8Array }) => {
                                assert.equal(
                                    new TextDecoder()
                                        .decode(row.messageBytes)
                                        .includes('synthetic-url'),
                                    false
                                )
                                writes += 1
                                if (mode === 'private_failure')
                                    throw new Error('synthetic-private-failure')
                            },
                            persistThreadAsync: async () => undefined,
                            persistContactAsync: async () => undefined
                        } as never,
                        emitEvent: () => undefined,
                        onProcessed: () => current.sendHistorySyncReceipt()
                    } as never,
                    input,
                    {
                        limits,
                        signal,
                        onBatch: async (batch) => {
                            assert.equal(lifecycle.onStart(), true)
                            assert.equal(
                                batch.sourceFamilyDispositions.filter(
                                    (row) => row.disposition === 'EXCLUDED_BY_ACCEPTED_SCOPE'
                                ).length,
                                7
                            )
                            assert.equal(batch.payloadIntegrityVerified, true)
                            entered.resolve()
                            try {
                                await release.promise
                                if (mode === 'reject') throw new Error('synthetic-callback-failure')
                            } finally {
                                lifecycle.onSettled()
                            }
                            if (signal.aborted) throw new Error('synthetic-cancelled')
                            current.complete()
                        }
                    }
                )
            },
            (reason) => failures.push(reason)
        )
        const historyMessage: Proto.IMessage = {
            protocolMessage: {
                type: proto.Message.ProtocolMessage.Type.HISTORY_SYNC_NOTIFICATION,
                historySyncNotification: {
                    syncType: 3,
                    initialHistBootstrapInlinePayload: gzipSync(acceptedDispositionsPayload())
                }
            }
        }
        const plaintext = (message: Proto.IMessage) => {
            const encoded = proto.Message.encode(message).finish()
            const padded = new Uint8Array(encoded.length + 1)
            padded.set(encoded)
            padded[encoded.length] = 1
            return padded
        }
        const node = (id: string) => ({
            tag: 'message',
            attrs: { id, from: 'synthetic-peer@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        })
        await handleIncomingMessageAck(node('opaque-history'), {
            logger,
            sendNode: async (value) => {
                sent.push(value.attrs.id + ':' + (value.attrs.type ?? 'delivery'))
            },
            captureBoundSendNode: () => {
                const captured = generation
                return async (value) => {
                    if (captured !== generation) throw new Error('synthetic-stale-generation')
                    sent.push(value.attrs.id + ':' + (value.attrs.type ?? 'delivery'))
                }
            },
            signalProtocol: { decryptMessage: async () => plaintext(historyMessage) } as never,
            scheduleHistorySync: (event, current) =>
                worker.enqueue(
                    event.message!.protocolMessage!.historySyncNotification!,
                    current,
                    () => settled.resolve()
                )
        })
        await entered.promise
        assert.equal(sent.length, 0)
        assert.equal(writes, 0)
        let liveEvents = 0
        await handleIncomingMessageAck(node('opaque-live'), {
            logger,
            signalProtocol: {
                decryptMessage: async () => plaintext({ conversation: 'synthetic-live' })
            } as never,
            emitIncomingMessage: () => {
                liveEvents += 1
            },
            sendNode: async (value) => {
                sent.push(value.attrs.id + ':' + (value.attrs.type ?? 'delivery'))
            }
        })
        assert.equal(liveEvents, 1)
        assert.deepEqual(sent, ['opaque-live:delivery'])
        if (mode === 'stale') generation += 1
        if (mode === 'cancel') await worker.cancelAndDrain()
        else if (mode === 'timeout') {
            // Library deadlines are intentionally unref'ed; the test owns a finite liveness handle.
            const keepAlive = setTimeout(() => undefined, 2_000)
            try {
                await settled.promise
            } finally {
                clearTimeout(keepAlive)
            }
        } else release.resolve()
        await settled.promise
        release.resolve()
        await new Promise<void>((done) => setImmediate(done))
        const historySends = sent.filter((value) => value.startsWith('opaque-history'))
        if (mode === 'success') {
            assert.deepEqual(historySends, ['opaque-history:delivery', 'opaque-history:hist_sync'])
            assert.equal(writes, 1)
        } else if (mode === 'private_failure') {
            assert.deepEqual(historySends, ['opaque-history:delivery'])
            assert.equal(writes, 1)
        } else {
            assert.deepEqual(historySends, [])
            if (mode === 'cancel' || mode === 'timeout') assert.equal(worker.getStatus(), 'FAULTED')
        }
        for (const evidence of [JSON.stringify(logs), JSON.stringify(failures)]) {
            for (const forbidden of [
                'synthetic-url',
                'synthetic-peer',
                'synthetic-group',
                'synthetic-direct'
            ])
                assert.equal(evidence.includes(forbidden), false)
        }
        await worker.cancelAndDrain()
    })
}
