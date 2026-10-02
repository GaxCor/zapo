import assert from 'node:assert/strict'
import { getEventListeners } from 'node:events'
import test from 'node:test'
import { gzipSync } from 'node:zlib'

import {
    getHistoryFailure,
    historyFailure,
    markHistoryFailure,
    waitHistoryCleanup
} from '@client/persistence/history-failure'
import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { WriteBehindPersistence } from '@client/persistence/WriteBehindPersistence'
import { WaClient } from '@client/WaClient'
import { createNoopLogger } from '@infra/log/types'
import { proto, type Proto } from '@proto'

const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 65_536,
    maxBatchBytes: 65_536,
    maxMessagesPerBatch: 64,
    maxSourceRecordsPerBatch: 512,
    timeoutMs: 300_000
}
const address = 'synthetic@s.whatsapp.net'
const message = (): Proto.IWebMessageInfo => ({
    key: { id: 'synthetic-id', remoteJid: address, fromMe: false },
    message: { conversation: 'synthetic-private-value' }
})
const history = (conversations: Proto.IConversation[] = []) =>
    proto.HistorySync.encode({ syncType: 3, conversations }).finish()
const conversation = (record = message()): Proto.IConversation => ({
    id: address,
    messages: [{ message: record }]
})

async function rejected(
    payload: Uint8Array,
    overrides: Partial<Proto.Message.IHistorySyncNotification> = {}
) {
    const store = { upsert: async () => undefined, upsertBatch: async () => undefined }
    const writeBehind = new WriteBehindPersistence(
        { messageStore: store as never, threadStore: store as never, contactStore: store as never },
        createNoopLogger()
    )
    let error: unknown
    try {
        await processHistorySyncNotification(
            {
                logger: createNoopLogger(),
                writeBehind,
                mediaTransfer: {} as never,
                emitEvent: () => assert.fail('failed batch emitted chunk'),
                onProcessed: async () => assert.fail('failed batch sent hist_sync')
            },
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(payload), ...overrides },
            {
                limits: LIMITS,
                signal: new AbortController().signal,
                onBatch: async () => assert.fail('failed batch invoked callback')
            }
        )
        assert.fail('fixture must reject')
    } catch (caught) {
        error = caught
    }
    await writeBehind.destroy()
    const record = getHistoryFailure(error)
    assert.ok(record)
    assert.deepEqual(Object.keys(record.diagnostic).sort(), ['field', 'reason', 'type', 'wire'])
    const encoded = JSON.stringify(record)
    for (const value of [
        address,
        'synthetic-id',
        'synthetic-private-value',
        'Error:',
        'stack',
        'raw'
    ])
        assert.equal(encoded.includes(value), false)
    return record
}

const cases: Array<{
    name: string
    payload: () => Uint8Array
    reason: string
    type: string | null
    field: number | null
    wire: number | null
}> = [
    {
        name: 'notification mismatch',
        payload: () => proto.HistorySync.encode({ syncType: 2 }).finish(),
        reason: 'notification_payload_sync_type_mismatch',
        type: 'HistorySync',
        field: 1,
        wire: 0
    },
    {
        name: 'missing conversation address',
        payload: () => history([{}]),
        reason: 'missing_conversation_address',
        type: 'Conversation',
        field: 1,
        wire: null
    },
    {
        name: 'unclassified address',
        payload: () => history([{ id: 'synthetic@unknown.invalid' }]),
        reason: 'unclassified_conversation_address',
        type: 'Conversation',
        field: 1,
        wire: 2
    },
    {
        name: 'missing record message',
        payload: () => history([{ id: address, messages: [{}] }]),
        reason: 'unknown_or_missing_message_record',
        type: 'HistorySyncMsg',
        field: 1,
        wire: null
    },
    {
        name: 'missing identity',
        payload: () => {
            const record = message()
            delete record.key!.id
            return history([conversation(record)])
        },
        reason: 'direct_message_missing_required_identity_or_content',
        type: 'MessageKey',
        field: 3,
        wire: null
    },
    {
        name: 'address conflict',
        payload: () => {
            const record = message()
            record.key!.remoteJid = 'other@s.whatsapp.net'
            return history([conversation(record)])
        },
        reason: 'direct_message_address_conflict',
        type: 'MessageKey',
        field: 1,
        wire: 2
    },
    {
        name: 'content unclassified',
        payload: () => {
            const record = message()
            record.message = {}
            return history([conversation(record)])
        },
        reason: 'direct_message_content_unclassified',
        type: 'Message',
        field: null,
        wire: null
    },
    {
        name: 'unsupported root',
        payload: () => new Uint8Array([...history(), 0x18, 0]),
        reason: 'unsupported_history_root_family',
        type: 'HistorySync',
        field: 3,
        wire: 0
    },
    {
        name: 'malformed root family',
        payload: () => new Uint8Array([...history(), 0x28 + 2, 0]),
        reason: 'malformed_history_root_family',
        type: 'HistorySync',
        field: 5,
        wire: 2
    },
    {
        name: 'unknown conversation field',
        payload: () => new Uint8Array([...history(), 0x12, 6, 0x0a, 1, 0x78, 0xa0, 6, 0]),
        reason: 'unknown_conversation_field',
        type: 'Conversation',
        field: 100,
        wire: 0
    },
    {
        name: 'malformed conversation field',
        payload: () => new Uint8Array([...history(), 0x12, 2, 0x10, 0]),
        reason: 'malformed_conversation_field',
        type: 'Conversation',
        field: 2,
        wire: 0
    }
]

for (const item of cases)
    test(`fixed diagnostic: ${item.name}`, async () => {
        const record = await rejected(item.payload())
        assert.deepEqual(record.diagnostic, {
            reason: item.reason,
            type: item.type,
            field: item.field,
            wire: item.wire
        })
        assert.ok(
            record.stages.includes('POST_EOF_GATE_REJECTED') ||
                record.stages.includes('WALK_FAILED')
        )
    })

for (const [target, reason, type] of [
    ['conversation', 'unknown_conversation_fields', 'Conversation'],
    ['key', 'unknown_message_fields', 'MessageKey'],
    ['extended', 'unknown_extended_text_fields', 'Message.ExtendedTextMessage']
] as const)
    test(`defensive post-EOF ${reason} retains exact structural provenance`, async (t) => {
        const decode =
            target === 'conversation' ? proto.Conversation.decode : proto.HistorySyncMsg.decode
        if (target === 'conversation')
            t.mock.method(
                proto.Conversation,
                'decode',
                (...args: Parameters<typeof proto.Conversation.decode>) => {
                    const decoded = decode(...args) as Proto.Conversation
                    decoded.$unknowns = [new Uint8Array([0x98, 6, 1])]
                    return decoded
                }
            )
        else
            t.mock.method(
                proto.HistorySyncMsg,
                'decode',
                (...args: Parameters<typeof proto.HistorySyncMsg.decode>) => {
                    const decoded = decode(...args) as Proto.HistorySyncMsg
                    if (target === 'key')
                        decoded.message!.key!.$unknowns = [new Uint8Array([0x98, 6, 1])]
                    else
                        decoded.message!.message = {
                            extendedTextMessage: {
                                text: 'synthetic',
                                $unknowns: [new Uint8Array([0x98, 6, 1])]
                            }
                        }
                    return decoded
                }
            )
        const record = await rejected(history([conversation()]))
        assert.deepEqual(record.diagnostic, { reason, type, field: 99, wire: 0 })
        assert.ok(record.stages.includes('POST_EOF_GATE_REJECTED'))
    })

test('first deferred failure stays authoritative rather than the last family observed', async () => {
    const record = await rejected(new Uint8Array([...history(), 0x18, 0, 0x08, 2]))
    assert.equal(record.diagnostic.reason, 'unsupported_history_root_family')
    assert.equal(record.diagnostic.field, 3)
})

test('batch projection failure is fixed, with no dynamic exception value', async () => {
    const record = await rejected(history(), {
        oldestMsgInChunkTimestampSec: {
            toString: () => {
                throw new Error('synthetic secret')
            }
        } as never
    })
    assert.equal(record.diagnostic.reason, 'BATCH_PROJECTION_FAILED')
    assert.ok(record.stages.includes('BATCH_PROJECTION_FAILED'))
})

test('callback admission failure is distinguished without invoking application callback', async () => {
    const events: string[] = []
    const fake = {
        options: {
            history: {
                boundaryLimits: LIMITS,
                onBatch: async () => assert.fail('application callback admitted')
            }
        },
        logger: createNoopLogger(),
        mediaTransfer: {},
        writeBehind: {},
        stores: {},
        emit: () => undefined,
        deps: { authClient: { getCurrentCredentials: () => null }, trustedContactToken: {} }
    }
    const prototype = WaClient.prototype as unknown as {
        processHistorySyncBoundary: (...args: unknown[]) => Promise<void>
    }
    await assert.rejects(
        prototype.processHistorySyncBoundary.call(
            fake,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(history()) },
            new AbortController().signal,
            {
                complete: () => events.push('COMPLETE'),
                sendHistorySyncReceipt: async () => {
                    events.push('HIST_SYNC')
                }
            },
            {
                onStart: () => false,
                onSettled: () => assert.fail('callback settled without admission')
            }
        ),
        (error) => {
            const record = getHistoryFailure(error)
            assert.equal(record?.diagnostic.reason, 'CALLBACK_ADMISSION_REJECTED')
            assert.ok(record?.stages.includes('CALLBACK_ADMISSION_REJECTED'))
            return true
        }
    )
    assert.deepEqual(events, [])
})

test('fixed failure projection preserves error identity and first reason through later abort', () => {
    const error = historyFailure('unknown_message_fields', 'Message', 99, 0)
    assert.equal(markHistoryFailure(error, 'CANCELLED', 'CLEANUP_ABORTED'), error)
    assert.equal(getHistoryFailure(error)?.diagnostic.reason, 'unknown_message_fields')
    assert.equal(getHistoryFailure(new Error('not a fixed diagnostic')), null)
})

test('bounded cleanup removes signal listener on abort without claiming provider cancellation', async () => {
    const signal = new AbortController()
    const pending = new Promise<void>(() => undefined)
    const waiting = waitHistoryCleanup(pending, signal.signal)
    signal.abort()
    assert.equal(await waiting, false)
    assert.equal(getEventListeners(signal.signal, 'abort').length, 0)
})
