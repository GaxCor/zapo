import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import { Readable } from 'node:stream'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'
import { deflateSync, gzipSync } from 'node:zlib'

import { openHistoryBlobStream } from '@client/persistence/history-blob'
import { getHistoryFailure } from '@client/persistence/history-failure'
import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { HistorySyncBoundaryWorker } from '@client/persistence/HistorySyncBoundaryWorker'
import { WriteBehindPersistence } from '@client/persistence/WriteBehindPersistence'
import { WaClient } from '@client/WaClient'
import { createNoopLogger } from '@infra/log/types'
import { WaMediaCrypto } from '@media/crypto/WaMediaCrypto'
import { handleIncomingMessageAck } from '@message/primitives/incoming'
import { proto } from '@proto'

const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 2,
    maxQueuedBytes: 65_536,
    maxBatchBytes: 65_536,
    maxMessagesPerBatch: 64,
    maxSourceRecordsPerBatch: 512,
    timeoutMs: 300_000
}
const valid = () =>
    proto.HistorySync.encode({
        syncType: 3,
        conversations: [
            {
                id: 'synthetic@s.whatsapp.net',
                ephemeralExpiration: 60,
                messages: [
                    {
                        message: {
                            key: {
                                id: 'synthetic',
                                remoteJid: 'synthetic@s.whatsapp.net',
                                fromMe: false
                            },
                            message: { conversation: 'synthetic' }
                        }
                    }
                ]
            }
        ]
    }).finish()

function deferred() {
    let resolve!: () => void
    let reject!: (error: Error) => void
    const promise = new Promise<void>((yes, no) => {
        resolve = yes
        reject = no
    })
    return { promise, resolve, reject }
}

function fixture() {
    const events: string[] = []
    const store = { upsert: async () => undefined, upsertBatch: async () => undefined }
    const writeBehind = new WriteBehindPersistence(
        { messageStore: store as never, threadStore: store as never, contactStore: store as never },
        createNoopLogger()
    )
    const deps = {
        logger: createNoopLogger(),
        writeBehind,
        mediaTransfer: {} as never,
        emitEvent: () => events.push('CHUNK'),
        onProcessed: async () => {
            events.push('HIST_SYNC')
        }
    }
    return { events, writeBehind, deps }
}

async function within<T>(promise: Promise<T>, timeout = 1_000): Promise<T> {
    let timer!: NodeJS.Timeout
    try {
        return await Promise.race([
            promise,
            new Promise<never>((_resolve, reject) => {
                timer = setTimeout(() => reject(new Error('synthetic test ceiling')), timeout)
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

async function liveReceipt(): Promise<string[]> {
    const sent: string[] = []
    const encoded = proto.Message.encode({ conversation: 'synthetic live' }).finish()
    const padded = new Uint8Array(encoded.length + 1)
    padded.set(encoded)
    padded[padded.length - 1] = 1
    const handled = await handleIncomingMessageAck(
        {
            tag: 'message',
            attrs: { id: 'synthetic-live', from: 'other@s.whatsapp.net' },
            content: [{ tag: 'enc', attrs: { type: 'msg' }, content: new Uint8Array([1]) }]
        },
        {
            logger: createNoopLogger(),
            signalProtocol: { decryptMessage: async () => padded } as never,
            sendNode: async (node) => {
                assert.equal(node.attrs.id, 'synthetic-live')
                sent.push(node.tag)
            },
            emitIncomingMessage: () => {
                sent.push('DIRECT')
            }
        }
    )
    assert.equal(handled, true)
    return sent
}

for (const verification of ['resolve', 'reject', 'pending', 'abort'] as const) {
    test(`consumer rejection promptly cancels verification=${verification}, preserves original failure`, async () => {
        const f = fixture()
        const original = new Error('synthetic consumer failure contains no diagnostic data')
        f.writeBehind.ownHistoryOperation = () => {
            throw original
        }
        const metadata = deferred()
        if (verification === 'resolve') metadata.resolve()
        if (verification === 'reject') metadata.reject(new Error('synthetic verification'))
        let cancelled = false
        let transferSignal!: AbortSignal
        f.deps.mediaTransfer = {
            downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
                transferSignal = request.signal
                const onAbort = () => {
                    cancelled = true
                    metadata.reject(new Error('synthetic cancelled producer'))
                }
                request.signal.addEventListener('abort', onAbort, { once: true })
                const plaintext = Readable.from([gzipSync(valid())])
                plaintext.once('close', () => request.signal.removeEventListener('abort', onAbort))
                // Verification may still be pending after EOF, so retain its own cancellation owner.
                const verificationAbort = () => {
                    cancelled = true
                    metadata.reject(new Error('synthetic verified abort'))
                }
                request.signal.addEventListener('abort', verificationAbort, { once: true })
                metadata.promise.then(
                    () => request.signal.removeEventListener('abort', verificationAbort),
                    () => request.signal.removeEventListener('abort', verificationAbort)
                )
                return { plaintext, metadata: metadata.promise }
            }
        } as never
        const signal = new AbortController()
        const started = performance.now()
        const operation = processHistorySyncNotification(
            f.deps,
            {
                syncType: 3,
                directPath: '/synthetic',
                mediaKey: new Uint8Array(32),
                fileSha256: new Uint8Array(32),
                fileEncSha256: new Uint8Array(32)
            },
            {
                limits: LIMITS,
                signal: signal.signal,
                onBatch: async () => {
                    f.events.push('CALLBACK')
                }
            }
        )
        await assert.rejects(within(operation), (error) => error === original)
        assert.ok(performance.now() - started < 1_000)
        if (verification === 'pending' || verification === 'abort') assert.equal(cancelled, true)
        assert.equal(transferSignal.aborted, true)
        assert.equal(getEventListeners(signal.signal, 'abort').length, 0)
        assert.deepEqual(f.events, [])
        assert.ok(getHistoryFailure(original)?.stages.includes('CLEANUP_CANCEL_REQUESTED'))
        await f.writeBehind.destroy()
    })
}

for (const outcome of ['resolve', 'reject', 'settled', 'rejected'] as const) {
    test(`failed batch retains private cache work through ${outcome} without blocking live traffic`, async () => {
        const f = fixture()
        const pending = deferred()
        pending.promise.catch(() => undefined)
        if (outcome === 'settled') pending.resolve()
        if (outcome === 'rejected') pending.reject(new Error('synthetic private rejection'))
        const settled = deferred()
        let processError: unknown
        let failureMs = 0
        const started = performance.now()
        const worker = new HistorySyncBoundaryWorker(
            LIMITS,
            async (notification, signal) => {
                try {
                    await processHistorySyncNotification(
                        {
                            ...f.deps,
                            chatMetadataStore: {
                                upsertChatMetadata: () => pending.promise
                            } as never
                        },
                        notification,
                        {
                            limits: LIMITS,
                            signal,
                            onBatch: async () => {
                                f.events.push('CALLBACK')
                            }
                        }
                    )
                } catch (error) {
                    processError = error
                    failureMs = performance.now() - started
                    throw error
                }
            },
            (reason) => {
                assert.equal(reason, 'processing_failed')
            }
        )
        worker.enqueue(
            {
                syncType: 3,
                initialHistBootstrapInlinePayload: gzipSync(new Uint8Array([...valid(), 0]))
            },
            {
                complete: () => f.events.push('COMPLETE'),
                fail: () => {
                    if (!f.events.includes('FAIL')) f.events.push('FAIL')
                },
                sendHistorySyncReceipt: async () => {
                    f.events.push('HIST_SYNC')
                }
            },
            settled.resolve
        )
        await delay(10)
        const live = await liveReceipt()
        assert.deepEqual(live, ['DIRECT', 'receipt'])
        await within(settled.promise)
        assert.ok(failureMs < 1_000, `failure cleanup ${failureMs}ms vs 300000ms deadline`)
        assert.equal(worker.running(), 0)
        assert.equal(worker.getStatus(), 'AVAILABLE')
        assert.deepEqual(f.events, ['FAIL'])
        const remaining = await f.writeBehind.flush(0)
        assert.equal(
            remaining.history.remaining,
            outcome === 'resolve' || outcome === 'reject' ? 1 : 0
        )
        assert.equal(getHistoryFailure(processError)?.diagnostic.reason, 'WALK_FAILED')
        if (outcome === 'resolve') pending.resolve()
        if (outcome === 'reject') pending.reject(new Error('synthetic late private rejection'))
        await f.writeBehind.destroy()
        assert.equal((await f.writeBehind.flush(0)).history.remaining, 0)
        assert.deepEqual(f.events, ['FAIL'])
        await worker.cancelAndDrain()
    })
}

test('actual crypto pump cancels stalled encrypted source on parser rejection before worker timeout', async () => {
    const f = fixture()
    const bytes = new Uint8Array(8_192)
    let seed = 12345
    for (let i = 1; i < bytes.length; i += 1) {
        seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0
        bytes[i] = seed >>> 24
    }
    const key = new Uint8Array(32)
    const encrypted = await WaMediaCrypto.encryptBytes('history', key, deflateSync(bytes), {
        sidecar: false
    })
    let sent = false
    const source = new Readable({
        read() {
            if (!sent) {
                sent = true
                this.push(encrypted.ciphertextHmac.subarray(0, 2_048))
            }
        }
    })
    let plaintext!: Readable
    let metadataState = 'PENDING'
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async () => {
            const decrypted = await WaMediaCrypto.decryptReadable(source, {
                mediaType: 'history',
                mediaKey: key
            })
            plaintext = decrypted.plaintext
            decrypted.metadata.then(
                () => {
                    metadataState = 'RESOLVED'
                },
                () => {
                    metadataState = 'REJECTED'
                }
            )
            return decrypted
        }
    } as never
    const started = performance.now()
    await assert.rejects(
        within(
            processHistorySyncNotification(
                f.deps,
                {
                    syncType: 3,
                    directPath: '/synthetic',
                    mediaKey: key,
                    fileSha256: key,
                    fileEncSha256: key
                },
                {
                    limits: LIMITS,
                    signal: new AbortController().signal,
                    onBatch: async () => {
                        f.events.push('CALLBACK')
                    }
                }
            )
        )
    )
    assert.ok(performance.now() - started < 1_000)
    assert.equal(source.destroyed, true)
    assert.equal(plaintext.destroyed, true)
    assert.equal(metadataState, 'REJECTED')
    assert.equal(plaintext.listenerCount('drain'), 0)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('existing callback -> ordinary receipt -> private settlement -> hist_sync order is unchanged beside live AC-002', async () => {
    const f = fixture()
    const held = deferred()
    const entered = deferred()
    const privateWrite = deferred()
    const privateStarted = deferred()
    const events: string[] = []
    f.writeBehind.persistMessageAsync = () => {
        events.push('PRIVATE_START')
        privateStarted.resolve()
        return privateWrite.promise
    }
    const fake = {
        options: {
            history: {
                boundaryLimits: LIMITS,
                onBatch: async () => {
                    events.push('CALLBACK_ENTER')
                    entered.resolve()
                    await held.promise
                    events.push('CALLBACK_SUCCESS')
                }
            }
        },
        logger: createNoopLogger(),
        mediaTransfer: f.deps.mediaTransfer,
        stores: {},
        writeBehind: f.writeBehind,
        emit: () => events.push('CHUNK'),
        deps: { authClient: { getCurrentCredentials: () => null }, trustedContactToken: {} }
    }
    const prototype = WaClient.prototype as unknown as {
        processHistorySyncBoundary: (...args: unknown[]) => Promise<void>
    }
    const running = prototype.processHistorySyncBoundary.call(
        fake,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(valid()) },
        new AbortController().signal,
        {
            complete: () => events.push('ORDINARY_RECEIPT'),
            sendHistorySyncReceipt: async () => {
                events.push('HIST_SYNC')
            }
        },
        { onStart: () => true, onSettled: () => undefined }
    )
    await within(entered.promise)
    assert.deepEqual(events, ['CALLBACK_ENTER'])
    assert.deepEqual(await liveReceipt(), ['DIRECT', 'receipt'])
    held.resolve()
    await within(privateStarted.promise)
    assert.deepEqual(events, [
        'CALLBACK_ENTER',
        'CALLBACK_SUCCESS',
        'ORDINARY_RECEIPT',
        'PRIVATE_START'
    ])
    events.push('PRIVATE_SETTLED')
    privateWrite.resolve()
    await within(running)
    assert.deepEqual(events, [
        'CALLBACK_ENTER',
        'CALLBACK_SUCCESS',
        'ORDINARY_RECEIPT',
        'PRIVATE_START',
        'PRIVATE_SETTLED',
        'CHUNK',
        'HIST_SYNC'
    ])
    await f.writeBehind.destroy()
})

test('inline teardown is network-free, idempotent and removes owned abort listeners', async () => {
    const controller = new AbortController()
    const blob = await openHistoryBlobStream(
        {} as never,
        {},
        'history',
        'synthetic',
        gzipSync(valid()),
        { signal: controller.signal }
    )
    blob.cancel()
    blob.cancel()
    await within(blob.settled)
    assert.equal(blob.inflated.destroyed, true)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('already aborted history does not open a download or invoke callback', async () => {
    const f = fixture()
    const controller = new AbortController()
    controller.abort()
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(valid()) },
            {
                limits: LIMITS,
                signal: controller.signal,
                onBatch: async () => assert.fail('callback after abort')
            }
        )
    )
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
    await f.writeBehind.destroy()
})

test('verification rejection after EOF has fixed integrity category and no callback', async () => {
    const f = fixture()
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async () => ({
            plaintext: Readable.from([
                gzipSync(proto.HistorySync.encode({ syncType: 3 }).finish())
            ]),
            metadata: Promise.reject(new Error('synthetic MAC failure'))
        })
    } as never
    await assert.rejects(
        within(
            processHistorySyncNotification(
                f.deps,
                {
                    syncType: 3,
                    directPath: '/synthetic',
                    mediaKey: new Uint8Array(32),
                    fileSha256: new Uint8Array(32),
                    fileEncSha256: new Uint8Array(32)
                },
                {
                    limits: LIMITS,
                    signal: new AbortController().signal,
                    onBatch: async () => assert.fail('unverified callback')
                }
            )
        ),
        (error) => {
            assert.equal(getHistoryFailure(error)?.diagnostic.reason, 'VERIFICATION_REJECTED')
            return true
        }
    )
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('decompression failure cancels an independent pending verification producer', async () => {
    const f = fixture()
    const metadata = deferred()
    let cancelled = false
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
            request.signal.addEventListener(
                'abort',
                () => {
                    cancelled = true
                    metadata.reject(new Error('synthetic cancellation'))
                },
                { once: true }
            )
            return {
                plaintext: Readable.from([new Uint8Array([1, 2, 3, 4])]),
                metadata: metadata.promise
            }
        }
    } as never
    await assert.rejects(
        within(
            processHistorySyncNotification(
                f.deps,
                {
                    syncType: 3,
                    directPath: '/synthetic',
                    mediaKey: new Uint8Array(32),
                    fileSha256: new Uint8Array(32),
                    fileEncSha256: new Uint8Array(32)
                },
                {
                    limits: LIMITS,
                    signal: new AbortController().signal,
                    onBatch: async () => assert.fail('decompression failed')
                }
            )
        )
    )
    assert.equal(cancelled, true)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('abort during download is job-local and removes the linked caller listener', async () => {
    const controller = new AbortController()
    const opened = deferred()
    let child!: AbortSignal
    const operation = openHistoryBlobStream(
        {
            downloadAndDecryptStream: (request: { signal: AbortSignal }) => {
                child = request.signal
                opened.resolve()
                return new Promise((_resolve, reject) =>
                    request.signal.addEventListener(
                        'abort',
                        () => reject(new Error('synthetic download cancelled')),
                        { once: true }
                    )
                )
            }
        } as never,
        {
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        'history',
        'synthetic',
        undefined,
        { signal: controller.signal }
    )
    await opened.promise
    controller.abort()
    await assert.rejects(within(operation))
    assert.notEqual(child, controller.signal)
    assert.equal(child.aborted, true)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('destroyed inflated stream propagates cancellation to the independent verification owner', async () => {
    const metadata = deferred()
    const controller = new AbortController()
    let cancelled = false
    const plaintext = new Readable({ read() {} })
    const blob = await openHistoryBlobStream(
        {
            downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
                request.signal.addEventListener(
                    'abort',
                    () => {
                        cancelled = true
                        metadata.reject(new Error('synthetic producer cancelled'))
                    },
                    { once: true }
                )
                return { plaintext, metadata: metadata.promise }
            }
        } as never,
        {
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        'history',
        'synthetic',
        undefined,
        { signal: controller.signal }
    )
    blob.inflated.destroy()
    await within(blob.settled)
    assert.equal(cancelled, true)
    assert.equal(plaintext.destroyed, true)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
})

test('live DIRECT handler and matching ordinary receipt proceed while failed transfer cleanup is held', async () => {
    const f = fixture()
    const cancelling = deferred()
    const releaseProducer = deferred()
    const metadata = deferred()
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
            request.signal.addEventListener(
                'abort',
                () => {
                    cancelling.resolve()
                    void releaseProducer.promise.then(() =>
                        metadata.reject(new Error('synthetic producer terminated'))
                    )
                },
                { once: true }
            )
            return {
                plaintext: Readable.from([gzipSync(new Uint8Array([0]))]),
                metadata: metadata.promise
            }
        }
    } as never
    const finished = deferred()
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        async (input, signal) =>
            processHistorySyncNotification(f.deps, input, {
                limits: LIMITS,
                signal,
                onBatch: async () => assert.fail('failed history callback')
            }),
        (reason) => assert.equal(reason, 'processing_failed')
    )
    worker.enqueue(
        {
            syncType: 3,
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        {
            complete: () => assert.fail('failed history receipt'),
            fail: () => undefined,
            sendHistorySyncReceipt: async () => assert.fail('failed hist_sync')
        },
        finished.resolve
    )
    await within(cancelling.promise)
    assert.equal(worker.running(), 1)
    assert.deepEqual(await liveReceipt(), ['DIRECT', 'receipt'])
    assert.equal(worker.running(), 1)
    assert.deepEqual(f.events, [])
    releaseProducer.resolve()
    await within(finished.promise)
    assert.equal(worker.running(), 0)
    await worker.cancelAndDrain()
    await f.writeBehind.destroy()
})

test('abort during verification has fixed CANCELLED provenance and no receipt resurrection', async () => {
    const f = fixture()
    const metadata = deferred()
    const opened = deferred()
    const verificationWaiting = deferred()
    const controller = new AbortController()
    const addListener = controller.signal.addEventListener.bind(controller.signal)
    controller.signal.addEventListener = (type, listener, options) => {
        addListener(type, listener, options)
        // The blob owns the first listener. The verification gate owns the
        // second; waiting for it proves the walk reached EOF, without a sleep.
        if (type === 'abort' && getEventListeners(controller.signal, 'abort').length === 2) {
            verificationWaiting.resolve()
        }
    }
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
            request.signal.addEventListener(
                'abort',
                () => metadata.reject(new Error('synthetic owned abort')),
                { once: true }
            )
            opened.resolve()
            return {
                plaintext: Readable.from([
                    gzipSync(proto.HistorySync.encode({ syncType: 3 }).finish())
                ]),
                metadata: metadata.promise
            }
        }
    } as never
    const operation = processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        {
            limits: LIMITS,
            signal: controller.signal,
            onBatch: async () => assert.fail('callback after cancellation')
        }
    )
    await opened.promise
    await within(verificationWaiting.promise)
    controller.abort(new Error('synthetic private cancellation value'))
    await assert.rejects(within(operation), (error) => {
        assert.equal(getHistoryFailure(error)?.diagnostic.reason, 'CANCELLED')
        assert.equal(
            JSON.stringify(getHistoryFailure(error)).includes(
                'synthetic private cancellation value'
            ),
            false
        )
        return true
    })
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('caller abort while the walker waits for bytes has fixed CANCELLED provenance', async () => {
    const f = fixture()
    const metadata = deferred()
    const opened = deferred()
    const controller = new AbortController()
    const plaintext = new Readable({ read() {} })
    let child!: AbortSignal
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
            child = request.signal
            child.addEventListener('abort', () => metadata.reject(new Error('synthetic abort')), {
                once: true
            })
            opened.resolve()
            return { plaintext, metadata: metadata.promise }
        }
    } as never
    const operation = processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        {
            limits: LIMITS,
            signal: controller.signal,
            onBatch: async () => assert.fail('callback after cancellation')
        }
    )
    await opened.promise
    await new Promise<void>((done) => setImmediate(done))
    controller.abort()
    await assert.rejects(within(operation), (error) => {
        assert.equal(getHistoryFailure(error)?.diagnostic.reason, 'CANCELLED')
        return true
    })
    await assert.rejects(metadata.promise)
    assert.equal(plaintext.destroyed, true)
    assert.equal(child.aborted, true)
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('parser failure then caller abort preserves the original first failure', async () => {
    const f = fixture()
    const controller = new AbortController()
    const original = new Error('synthetic first consumer failure')
    f.writeBehind.ownHistoryOperation = () => {
        queueMicrotask(() => controller.abort(new Error('synthetic later cancellation')))
        throw original
    }
    await assert.rejects(
        within(
            processHistorySyncNotification(
                f.deps,
                { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(valid()) },
                {
                    limits: LIMITS,
                    signal: controller.signal,
                    onBatch: async () => assert.fail('callback after failure')
                }
            )
        ),
        (error) => {
            assert.equal(error, original)
            assert.equal(getHistoryFailure(error)?.diagnostic.reason, 'WALK_FAILED')
            return true
        }
    )
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})

test('caller abort during bounded cleanup does not resurrect a failed batch', async () => {
    const f = fixture()
    const metadata = deferred()
    const cleanupWaiting = deferred()
    const controller = new AbortController()
    let cancelled = false
    let linked = false
    const addListener = controller.signal.addEventListener.bind(controller.signal)
    controller.signal.addEventListener = (type, listener, options) => {
        addListener(type, listener, options)
        if (type !== 'abort') return
        if (!linked) linked = true
        else cleanupWaiting.resolve()
    }
    const original = new Error('synthetic parser failure')
    f.writeBehind.ownHistoryOperation = () => {
        throw original
    }
    f.deps.mediaTransfer = {
        downloadAndDecryptStream: async (request: { signal: AbortSignal }) => {
            request.signal.addEventListener(
                'abort',
                () => {
                    cancelled = true
                },
                { once: true }
            )
            return { plaintext: Readable.from([gzipSync(valid())]), metadata: metadata.promise }
        }
    } as never
    const operation = processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            directPath: '/synthetic',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        {
            limits: LIMITS,
            signal: controller.signal,
            onBatch: async () => assert.fail('callback after failure')
        }
    )
    await within(cleanupWaiting.promise)
    assert.equal(cancelled, true)
    controller.abort()
    await assert.rejects(within(operation), (error) => {
        assert.equal(error, original)
        assert.ok(getHistoryFailure(error)?.stages.includes('CLEANUP_ABORTED'))
        return true
    })
    // This deliberately non-cooperative synthetic producer is settled by its
    // test owner. A bounded wait alone never claims the producer was cancelled.
    metadata.reject(new Error('synthetic late verification'))
    await new Promise<void>((done) => setImmediate(done))
    assert.equal(getEventListeners(controller.signal, 'abort').length, 0)
    assert.deepEqual(f.events, [])
    await f.writeBehind.destroy()
})
