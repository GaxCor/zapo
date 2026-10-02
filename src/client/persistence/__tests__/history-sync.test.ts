import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import test from 'node:test'
import { promisify } from 'node:util'
import { gzip } from 'node:zlib'

import { openHistoryBlobStream } from '@client/persistence/history-blob'
import {
    CONVERSATION_FIELDS,
    HISTORY_SYNC_FIELDS,
    processHistorySyncNotification
} from '@client/persistence/history-sync'
import { WriteBehindPersistence } from '@client/persistence/WriteBehindPersistence'
import type { WaHistorySyncBatchV1, WaHistorySyncMessageObservation } from '@client/types'
import { createNoopLogger } from '@infra/log/types'
import { proto, type Proto } from '@proto'
import type { WaStoredContactRecord } from '@store/contracts/contact.store'
import type { WaStoredMessageRecord } from '@store/contracts/message.store'
import type { WaStoredThreadRecord } from '@store/contracts/thread.store'
import { toBytesView } from '@util/bytes'
import { readProtoVarint } from '@util/protoscan'

const gzipAsync = promisify(gzip)

function encodeVarint(value: number): Uint8Array {
    const bytes: number[] = []
    let remaining = value
    while (remaining > 0x7f) {
        bytes.push((remaining & 0x7f) | 0x80)
        remaining = Math.floor(remaining / 128)
    }
    bytes.push(remaining)
    return Uint8Array.from(bytes)
}

function concatenate(...parts: readonly Uint8Array[]): Uint8Array {
    const result = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0))
    let offset = 0
    for (const part of parts) {
        result.set(part, offset)
        offset += part.byteLength
    }
    return result
}

function encodeBytesField(fieldNumber: number, value: Uint8Array): Uint8Array {
    return concatenate(encodeVarint(fieldNumber * 8 + 2), encodeVarint(value.byteLength), value)
}

function encodeStringField(fieldNumber: number, value: string): Uint8Array {
    return encodeBytesField(fieldNumber, new TextEncoder().encode(value))
}

function encodeVarintField(fieldNumber: number, value: number): Uint8Array {
    return concatenate(encodeVarint(fieldNumber * 8), encodeVarint(value))
}

type SyntheticUnknownNode =
    | 'history_sync_msg'
    | 'web_message_info'
    | 'message_key'
    | 'message_container'
    | 'extended_text_message'
    | 'extended_text_context_info'

function buildHistoryWithUnknown(node: SyntheticUnknownNode): Uint8Array {
    const unknownField99 = encodeVarintField(99, 1)
    const address = '5511000000001@s.whatsapp.net'
    const contextInfoField = encodedFieldNumber(proto.Message.ExtendedTextMessage, {
        contextInfo: {}
    })
    const extendedText = concatenate(
        encodeStringField(1, 'visible text'),
        ...(node === 'extended_text_message' ? [unknownField99] : []),
        ...(node === 'extended_text_context_info'
            ? [encodeBytesField(contextInfoField, unknownField99)]
            : [])
    )
    const message = concatenate(
        encodeBytesField(6, extendedText),
        ...(node === 'message_container' ? [unknownField99] : [])
    )
    const key = concatenate(
        encodeStringField(1, address),
        encodeVarintField(2, 0),
        encodeStringField(3, 'msg-1'),
        ...(node === 'message_key' ? [unknownField99] : [])
    )
    const webMessage = concatenate(
        encodeBytesField(1, key),
        encodeBytesField(2, message),
        encodeVarintField(3, 1_700_000_000),
        ...(node === 'web_message_info' ? [unknownField99] : [])
    )
    const historyMessage = concatenate(
        encodeBytesField(1, webMessage),
        ...(node === 'history_sync_msg' ? [unknownField99] : [])
    )
    const conversation = concatenate(
        encodeStringField(1, address),
        encodeBytesField(2, historyMessage)
    )
    return concatenate(
        encodeVarintField(1, proto.HistorySync.HistorySyncType.RECENT),
        encodeBytesField(2, conversation)
    )
}

/**
 * Reads a field number back out of the generated encoder: encode a message
 * carrying only that field, then decode the leading tag. Keeps the hardcoded
 * numbers pinned to the code that actually serializes the wire, with no
 * reflection metadata to depend on.
 */
function encodedFieldNumber(
    type: { encode: (value: never) => { finish: () => Uint8Array } },
    value: unknown
): number {
    const bytes = type.encode(value as never).finish()
    assert.ok(bytes.length > 0, 'the probe field must actually be encoded')
    return Math.floor(readProtoVarint(bytes, 0, bytes.length).value / 8)
}

function createThreadCapture(): {
    readonly writes: WaStoredThreadRecord[]
    readonly writeBehind: WriteBehindPersistence
} {
    const writes: WaStoredThreadRecord[] = []
    const writeBehind = new WriteBehindPersistence(
        {
            messageStore: {
                upsert: async () => undefined,
                upsertBatch: async () => undefined
            } as never,
            threadStore: {
                upsert: async (record: WaStoredThreadRecord) => {
                    writes.push(record)
                },
                upsertBatch: async (records: readonly WaStoredThreadRecord[]) => {
                    writes.push(...records)
                }
            } as never,
            contactStore: {
                upsert: async () => undefined,
                upsertBatch: async () => undefined
            } as never
        },
        createNoopLogger()
    )
    return { writes, writeBehind }
}

interface TokenConversation {
    readonly jid: string
    readonly tcToken?: Uint8Array | null
    readonly tcTokenTimestamp?: number | null
}

interface Capture {
    readonly order: string[]
    readonly messages: WaStoredMessageRecord[]
    readonly threads: WaStoredThreadRecord[]
    readonly contacts: WaStoredContactRecord[]
    readonly events: unknown[]
    readonly acked: number[]
    readonly salts: Uint8Array[]
    readonly tokens: TokenConversation[]
}

function createCapture(meJid?: string): { readonly capture: Capture; readonly deps: unknown } {
    const capture: Capture = {
        order: [],
        messages: [],
        threads: [],
        contacts: [],
        events: [],
        acked: [],
        salts: [],
        tokens: []
    }
    const deps = {
        logger: createNoopLogger(),
        mediaTransfer: {} as never,
        writeBehind: {
            ownHistoryOperation: (start: () => Promise<void>) => start(),
            historyOperationsSettled: () => false,
            persistMessageAsync: async (record: WaStoredMessageRecord) => {
                capture.order.push(`msg:${record.id}`)
                capture.messages.push(record)
            },
            persistThreadAsync: async (record: WaStoredThreadRecord) => {
                capture.order.push(`thread:${record.jid}`)
                capture.threads.push(record)
            },
            persistContactAsync: async (record: WaStoredContactRecord) => {
                capture.contacts.push(record)
            }
        } as never,
        emitEvent: (_type: string, payload: unknown) => {
            capture.events.push(payload)
        },
        onNctSalt: async (salt: Uint8Array) => {
            capture.salts.push(salt)
        },
        onPrivacyTokens: async (conversations: readonly TokenConversation[]) => {
            capture.tokens.push(...conversations)
        },
        onProcessed: async (syncType: number) => {
            capture.acked.push(syncType)
        },
        meJid
    }
    return { capture, deps }
}

async function buildInlineNotification(
    conversation: proto.IConversation
): Promise<proto.Message.IHistorySyncNotification> {
    const payload = proto.HistorySync.encode({
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        conversations: [conversation]
    }).finish()
    return {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(payload))
    }
}

test('history sync converts Conversation.ephemeralSettingTimestamp from ms to seconds', async () => {
    const { writes, writeBehind } = createThreadCapture()
    const notification = await buildInlineNotification({
        id: '5511999999999@s.whatsapp.net',
        ephemeralExpiration: 86_400,
        ephemeralSettingTimestamp: 1_751_808_692_000
    })

    await processHistorySyncNotification(
        {
            logger: createNoopLogger(),
            mediaTransfer: {} as never,
            writeBehind,
            emitEvent: () => undefined
        } as never,
        notification
    )
    await writeBehind.flush()

    const thread = writes.find((record) => record.jid === '5511999999999@s.whatsapp.net')
    assert.ok(thread, 'conversation should be persisted as a thread row')
    assert.equal(thread.ephemeralExpiration, 86_400)
    assert.equal(thread.ephemeralSettingTimestamp, 1_751_808_692)
})

test('history boundary preserves duplicate source observations before ID-only writes and excludes non-direct detail', async () => {
    const { capture, deps } = createCapture()
    const payload = proto.HistorySync.encode({
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        chunkOrder: 4,
        progress: 44,
        conversations: [
            {
                id: '5511000000001@s.whatsapp.net',
                messages: [
                    {
                        msgOrderId: 7,
                        message: {
                            key: {
                                remoteJid: '5511000000001@s.whatsapp.net',
                                id: 'DUPLICATE',
                                fromMe: false
                            },
                            message: { conversation: 'first' },
                            messageTimestamp: 1_700_000_001
                        }
                    }
                ]
            },
            {
                id: '5511000000002@s.whatsapp.net',
                messages: [
                    {
                        msgOrderId: 7,
                        message: {
                            key: {
                                remoteJid: '5511000000002@s.whatsapp.net',
                                id: 'DUPLICATE',
                                fromMe: false
                            },
                            message: { extendedTextMessage: { text: '' } },
                            messageTimestamp: 1_700_000_002
                        }
                    }
                ]
            },
            {
                id: '120363000000000000@g.us',
                messages: [
                    {
                        message: {
                            key: {
                                remoteJid: '120363000000000000@g.us',
                                id: 'PRIVATE-GROUP-ID',
                                fromMe: true,
                                participant: '5511999999999@s.whatsapp.net'
                            },
                            message: { conversation: 'private group body' },
                            messageTimestamp: 1_700_000_003
                        }
                    }
                ]
            }
        ]
    }).finish()
    const notification = {
        syncType: proto.Message.HistorySyncType.RECENT,
        chunkOrder: 5,
        progress: 43,
        originalMessageId: 'source-message-id',
        oldestMsgInChunkTimestampSec: 1_700_000_000,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(payload))
    }
    const signal = new AbortController().signal
    let callbackSawNoMessageWrites = false
    let observations: readonly WaHistorySyncMessageObservation[] = []

    await processHistorySyncNotification(deps as never, notification, {
        limits: {
            maxConcurrentBatches: 1,
            maxQueuedBatches: 1,
            maxQueuedBytes: 16_384,
            maxBatchBytes: 16_384,
            maxMessagesPerBatch: 10,
            maxSourceRecordsPerBatch: 128,
            timeoutMs: 1_000
        },
        signal,
        onBatch: async (batch) => {
            callbackSawNoMessageWrites = capture.messages.length === 0
            observations = batch.messageObservations
            assert.equal(batch.parseReachedEof, true)
            assert.equal(batch.payloadIntegrityVerified, true)
            assert.equal(batch.sourceHistoryCompleteness, 'UNKNOWN')
            assert.deepEqual(batch.metadataConflicts, [
                'notification_payload_chunk_order_mismatch',
                'notification_payload_progress_mismatch'
            ])
            assert.equal(batch.notification.oldestMsgInChunkTimestampSec, '1700000000')
            assert.equal(batch.decoded.chunkOrder, 4)
            assert.equal(batch.notification.chunkOrder, 5)
        }
    })

    assert.equal(callbackSawNoMessageWrites, true)
    assert.equal(observations.length, 3)
    assert.deepEqual(
        observations
            .slice(0, 2)
            .map((item) => [
                item.ordinal,
                item.conversationOrdinal,
                item.externalMessageId,
                item.fromMe,
                item.content
            ]),
        [
            [1, 1, 'DUPLICATE', false, { kind: 'TEXT', text: 'first' }],
            [2, 2, 'DUPLICATE', false, { kind: 'TEXT', text: '' }]
        ]
    )
    assert.equal(observations[2].disposition, 'EXCLUDED_GROUP')
    assert.equal(observations[2].externalMessageId, undefined)
    assert.equal(observations[2].fromMe, undefined)
    assert.equal(observations[2].timestampSeconds, undefined)
    assert.equal(JSON.stringify(observations).includes('private group body'), false)
    assert.equal(JSON.stringify(observations).includes('PRIVATE-GROUP-ID'), false)
    assert.deepEqual(
        capture.messages.map((record) => record.id),
        ['DUPLICATE', 'DUPLICATE', 'PRIVATE-GROUP-ID'],
        'post-callback Zapo mailbox writes may coalesce later, but callback sees both occurrences'
    )
})

test('history boundary rejects unknown root families before callback and receipt completion', async () => {
    const { capture, deps } = createCapture()
    const payload = proto.HistorySync.encode({
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        conversations: [{ id: '5511000000001@s.whatsapp.net', messages: [] }],
        recentStickers: [{}]
    }).finish()
    const inlinePayload = toBytesView(await gzipAsync(payload))
    let callbackCalled = false
    await assert.rejects(
        () =>
            processHistorySyncNotification(
                deps as never,
                {
                    syncType: proto.Message.HistorySyncType.RECENT,
                    initialHistBootstrapInlinePayload: inlinePayload
                },
                {
                    limits: {
                        maxConcurrentBatches: 1,
                        maxQueuedBatches: 0,
                        maxQueuedBytes: 0,
                        maxBatchBytes: 16_384,
                        maxMessagesPerBatch: 1,
                        maxSourceRecordsPerBatch: 32,
                        timeoutMs: 1_000
                    },
                    signal: new AbortController().signal,
                    onBatch: async () => {
                        callbackCalled = true
                    }
                }
            ),
        /unsupported_history_root_family/
    )
    assert.equal(callbackCalled, false)
    assert.deepEqual(capture.acked, [])
})

test('history boundary rejects unknown notification fields before callback and receipt completion', async () => {
    const { capture, deps } = createCapture()
    let callbackCalled = false
    await assert.rejects(
        () =>
            processHistorySyncNotification(
                deps as never,
                {
                    syncType: proto.Message.HistorySyncType.RECENT,
                    $unknowns: [new Uint8Array([0x78, 0x01])]
                },
                {
                    limits: {
                        maxConcurrentBatches: 1,
                        maxQueuedBatches: 0,
                        maxQueuedBytes: 0,
                        maxBatchBytes: 16_384,
                        maxMessagesPerBatch: 1,
                        maxSourceRecordsPerBatch: 32,
                        timeoutMs: 1_000
                    },
                    signal: new AbortController().signal,
                    onBatch: async () => {
                        callbackCalled = true
                    }
                }
            ),
        /history boundary notification contains unknown fields/
    )
    assert.equal(callbackCalled, false)
    assert.deepEqual(capture.acked, [])
})

test('history boundary rejects visible extended text when its consumed protobuf node has unknown fields', async () => {
    const { capture, deps } = createCapture()
    const rawPayload = buildHistoryWithUnknown('extended_text_message')
    const decoded = proto.HistorySync.decode(rawPayload)
    const nestedUnknowns =
        decoded.conversations?.[0]?.messages?.[0]?.message?.message?.extendedTextMessage?.$unknowns
    assert.equal(nestedUnknowns?.length, 1, 'synthetic field 99 must decode on ExtendedTextMessage')

    let callbackCalled = false
    const notification: proto.Message.IHistorySyncNotification = {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(rawPayload))
    }
    await assert.rejects(
        () =>
            processHistorySyncNotification(deps as never, notification, {
                limits: {
                    maxConcurrentBatches: 1,
                    maxQueuedBatches: 0,
                    maxQueuedBytes: 0,
                    maxBatchBytes: 16_384,
                    maxMessagesPerBatch: 8,
                    maxSourceRecordsPerBatch: 64,
                    timeoutMs: 1_000
                },
                signal: new AbortController().signal,
                onBatch: async () => {
                    callbackCalled = true
                }
            }),
        /unknown_extended_text_fields/
    )

    assert.equal(callbackCalled, false)
    assert.deepEqual(capture.messages, [], 'lossy observation must not reach mailbox writes')
    assert.deepEqual(capture.acked, [], 'failed boundary data must not be ordinary-acked')
    assert.equal(JSON.stringify(capture.events).includes('unknown'), false)
    assert.equal(JSON.stringify(capture.events).includes('visible text'), false)
})

test('history boundary rejects unknown fields nested below extended-text metadata', async () => {
    const rawPayload = buildHistoryWithUnknown('extended_text_context_info')
    const decoded = proto.HistorySync.decode(rawPayload)
    const nestedUnknowns =
        decoded.conversations?.[0]?.messages?.[0]?.message?.message?.extendedTextMessage
            ?.contextInfo?.$unknowns
    assert.equal(nestedUnknowns?.length, 1, 'synthetic field 99 must decode on ContextInfo')

    const { capture, deps } = createCapture()
    const notification: proto.Message.IHistorySyncNotification = {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(rawPayload))
    }
    let callbackCalled = false
    await assert.rejects(
        () =>
            processHistorySyncNotification(deps as never, notification, {
                limits: {
                    maxConcurrentBatches: 1,
                    maxQueuedBatches: 0,
                    maxQueuedBytes: 0,
                    maxBatchBytes: 16_384,
                    maxMessagesPerBatch: 8,
                    maxSourceRecordsPerBatch: 64,
                    timeoutMs: 1_000
                },
                signal: new AbortController().signal,
                onBatch: async () => {
                    callbackCalled = true
                }
            }),
        /unknown_extended_text_fields/
    )

    assert.equal(callbackCalled, false)
    assert.deepEqual(capture.messages, [])
    assert.deepEqual(capture.acked, [])
})

test('history boundary rejects unknown fields on every consumed message wrapper', async () => {
    const nodes: readonly SyntheticUnknownNode[] = [
        'history_sync_msg',
        'web_message_info',
        'message_key',
        'message_container'
    ]

    for (const node of nodes) {
        const { capture, deps } = createCapture()
        const rawPayload = buildHistoryWithUnknown(node)
        const inlinePayload = toBytesView(await gzipAsync(rawPayload))
        let callbackCalled = false
        await assert.rejects(
            () =>
                processHistorySyncNotification(
                    deps as never,
                    {
                        syncType: proto.Message.HistorySyncType.RECENT,
                        initialHistBootstrapInlinePayload: inlinePayload
                    },
                    {
                        limits: {
                            maxConcurrentBatches: 1,
                            maxQueuedBatches: 0,
                            maxQueuedBytes: 0,
                            maxBatchBytes: 16_384,
                            maxMessagesPerBatch: 8,
                            maxSourceRecordsPerBatch: 64,
                            timeoutMs: 1_000
                        },
                        signal: new AbortController().signal,
                        onBatch: async () => {
                            callbackCalled = true
                        }
                    }
                ),
            // Message tag 99 is pinned but this varint fixture has its wrong wire.
            /unknown|malformed_history_protocol_field_wire/
        )
        assert.equal(callbackCalled, false, `${node} must fail before batch acceptance`)
        assert.deepEqual(capture.messages, [], `${node} must not persist a lossy message`)
        assert.deepEqual(capture.acked, [], `${node} must not acknowledge failed history`)
    }
})

test('history sync keeps an already-seconds Conversation timestamp untouched', async () => {
    const { writes, writeBehind } = createThreadCapture()
    const notification = await buildInlineNotification({
        id: '5511777777777@s.whatsapp.net',
        ephemeralExpiration: 86_400,
        ephemeralSettingTimestamp: 1_751_808_692
    })

    await processHistorySyncNotification(
        {
            logger: createNoopLogger(),
            mediaTransfer: {} as never,
            writeBehind,
            emitEvent: () => undefined
        } as never,
        notification
    )
    await writeBehind.flush()

    const thread = writes.find((record) => record.jid === '5511777777777@s.whatsapp.net')
    assert.ok(thread)
    assert.equal(thread.ephemeralSettingTimestamp, 1_751_808_692)
})

test('history sync leaves ephemeralSettingTimestamp absent for a non-ephemeral chat', async () => {
    const { writes, writeBehind } = createThreadCapture()
    const notification = await buildInlineNotification({
        id: '5511888888888@s.whatsapp.net'
    })

    await processHistorySyncNotification(
        {
            logger: createNoopLogger(),
            mediaTransfer: {} as never,
            writeBehind,
            emitEvent: () => undefined
        } as never,
        notification
    )
    await writeBehind.flush()

    const thread = writes.find((record) => record.jid === '5511888888888@s.whatsapp.net')
    assert.ok(thread)
    assert.equal(thread.ephemeralSettingTimestamp, undefined)
})

test('streaming history sync is equivalent to the monolithic HistorySync decode', async () => {
    const historySync: Proto.IHistorySync = {
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        chunkOrder: 4,
        progress: 88,
        threadIdUserSecret: new Uint8Array([1, 2, 3]),
        nctSalt: new Uint8Array([9, 8, 7, 6]),
        pushnames: [
            { id: '5511111111111@s.whatsapp.net', pushname: 'Alice' },
            { id: '5522222222222@s.whatsapp.net', pushname: 'Bob' }
        ],
        phoneNumberToLidMappings: [
            { pnJid: '5511111111111@s.whatsapp.net', lidJid: '111@lid' },
            { pnJid: '5544444444444@s.whatsapp.net', lidJid: '444@lid' }
        ],
        inlineContacts: [
            { pnJid: '5522222222222@s.whatsapp.net', lidJid: '222@lid', fullName: 'Bob Silva' }
        ],
        statusV3Messages: [
            { key: { remoteJid: 'status@broadcast', id: 'S1' }, message: { conversation: 'st' } }
        ],
        pastParticipants: [{ groupJid: '123@g.us' }],
        conversations: [
            {
                id: '5511111111111@s.whatsapp.net',
                pnJid: '5511111111111@s.whatsapp.net',
                lidJid: '111@lid',
                tcToken: new Uint8Array([5, 5, 5]),
                name: 'Alice',
                unreadCount: 3,
                messages: [
                    {
                        message: {
                            key: { remoteJid: '5511111111111@s.whatsapp.net', id: 'M1' },
                            message: { conversation: 'oi' },
                            messageTimestamp: 1_722_000_000
                        }
                    },
                    {
                        message: {
                            key: { remoteJid: '5511111111111@s.whatsapp.net', id: 'M2' },
                            messageStubType: proto.WebMessageInfo.StubType.GROUP_PARTICIPANT_ADD
                        }
                    }
                ]
            },
            {
                id: '5533333333333@s.whatsapp.net',
                pnJid: '5533333333333@s.whatsapp.net',
                accountLid: '333@lid',
                messages: []
            },
            { id: '123@g.us', messages: [] }
        ]
    }
    const clean = proto.HistorySync.encode(historySync).finish()
    const unknownVarint = new Uint8Array([0xf8, 0x1f, 0x2a])
    const unknownLen = new Uint8Array([0xfa, 0x1f, 0x02, 0xde, 0xad])
    const blob = new Uint8Array(clean.length + unknownVarint.length + unknownLen.length)
    blob.set(unknownVarint, 0)
    blob.set(clean, unknownVarint.length)
    blob.set(unknownLen, unknownVarint.length + clean.length)

    const reference = proto.HistorySync.decode(blob)
    const { capture, deps } = createCapture()
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(blob))
    })

    const emitted = capture.events[0] as Record<string, number>
    assert.equal(emitted.conversationsCount, reference.conversations.length)
    assert.equal(emitted.pushnamesCount, reference.pushnames.length)
    assert.equal(emitted.inlineContactsCount, reference.inlineContacts.length)
    assert.equal(emitted.chunkOrder, reference.chunkOrder)
    assert.equal(emitted.progress, reference.progress)

    assert.deepEqual(
        capture.threads.map((record) => record.jid),
        reference.conversations.map((conversation) => conversation.id)
    )
    const alice = capture.threads[0]
    assert.equal(alice.name, 'Alice')
    assert.equal(alice.unreadCount, 3)

    assert.deepEqual(
        capture.messages.map((record) => record.id),
        ['M1']
    )
    assert.equal(capture.messages[0].threadJid, '5511111111111@s.whatsapp.net')
    assert.equal(capture.messages[0].timestampMs, 1_722_000_000_000)
    const messageBytes = capture.messages[0].messageBytes
    assert.ok(messageBytes, 'message payload must be persisted')
    assert.deepEqual(
        Array.from(messageBytes),
        Array.from(proto.Message.encode({ conversation: 'oi' }).finish())
    )

    const bob = capture.contacts.find((record) => record.pushName === 'Bob')
    assert.ok(bob, 'Bob pushname must be persisted')
    assert.equal(bob.jid, '222@lid', 'pushname must land on the canonical LID row')
    const aliceContact = capture.contacts.find((record) => record.pushName === 'Alice')
    assert.ok(aliceContact)
    assert.equal(aliceContact.jid, '111@lid')

    assert.deepEqual(
        capture.salts.map((salt) => Array.from(salt)),
        [Array.from(reference.nctSalt as Uint8Array)],
        'the chunk nctSalt must be handed over'
    )
    assert.deepEqual(
        capture.tokens.map((token) => [token.jid, Array.from(token.tcToken ?? [])]),
        [
            [
                '5511111111111@s.whatsapp.net',
                Array.from(reference.conversations[0].tcToken as Uint8Array)
            ]
        ],
        'privacy tokens must be collected from the conversations that carry them'
    )

    const standalone = capture.contacts.find((record) => record.jid === '444@lid')
    assert.ok(standalone, 'a mapping with no conversation or inline contact must still land')
    assert.equal(standalone.phoneNumber, '5544444444444@s.whatsapp.net')

    const firstMessage = capture.order.indexOf('msg:M1')
    const ownerThread = capture.order.indexOf('thread:5511111111111@s.whatsapp.net')
    assert.ok(firstMessage >= 0 && ownerThread >= 0)
    assert.ok(
        firstMessage < ownerThread,
        'messages must be written as they stream, before their conversation closes'
    )

    assert.deepEqual(capture.acked, [proto.Message.HistorySyncType.RECENT])
})

test('streaming history sync tolerates a Conversation whose id follows its messages', async () => {
    const messageField = proto.Conversation.encode({
        messages: [
            {
                message: {
                    key: { remoteJid: '5511444444444@s.whatsapp.net', id: 'REVERSED' },
                    message: { conversation: 'fora de ordem' },
                    messageTimestamp: 1_722_000_001
                }
            }
        ]
    }).finish()
    const idField = proto.Conversation.encode({ id: '5511444444444@s.whatsapp.net' }).finish()
    const conversation = new Uint8Array(messageField.length + idField.length)
    conversation.set(messageField, 0)
    conversation.set(idField, messageField.length)

    assert.ok(conversation.length < 128, 'fixture must fit single-byte length framing')
    const blob = new Uint8Array(2 + conversation.length)
    blob.set([0x12, conversation.length], 0)
    blob.set(conversation, 2)

    const { capture, deps } = createCapture()
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(blob))
    })

    assert.deepEqual(
        capture.messages.map((record) => [record.id, record.threadJid]),
        [['REVERSED', '5511444444444@s.whatsapp.net']]
    )
    const emitted = capture.events[0] as Record<string, number>
    assert.equal(
        emitted.messagesCount,
        capture.messages.length,
        'a parked message must be counted exactly once, when it is written'
    )
})

test('streaming history sync rejects a chunk whose MAC fails, without acking it', async () => {
    const payload = proto.HistorySync.encode({
        syncType: proto.HistorySync.HistorySyncType.RECENT,
        conversations: [{ id: '5511555555555@s.whatsapp.net', messages: [] }]
    }).finish()
    const gzipped = toBytesView(await gzipAsync(payload))

    const { capture, deps } = createCapture()
    const withTransfer = {
        ...(deps as Record<string, unknown>),
        mediaTransfer: {
            downloadAndDecryptStream: async () => ({
                plaintext: Readable.from([Buffer.from(gzipped)]),
                metadata: Promise.reject(new Error('media MAC mismatch'))
            })
        }
    }

    await assert.rejects(
        () =>
            processHistorySyncNotification(withTransfer as never, {
                syncType: proto.Message.HistorySyncType.RECENT,
                directPath: '/history',
                mediaKey: new Uint8Array(32),
                fileSha256: new Uint8Array(32),
                fileEncSha256: new Uint8Array(32)
            }),
        /media MAC mismatch/
    )
    assert.deepEqual(capture.acked, [], 'a chunk that failed verification must stay unacked')
    assert.deepEqual(capture.events, [], 'no chunk event may be emitted for unverified data')
})

test('history sync abort on a corrupt conversation settles pending writes', async () => {
    const validConversation = proto.Conversation.encode({
        id: '5511111111111@s.whatsapp.net',
        messages: [
            {
                message: {
                    key: { remoteJid: '5511111111111@s.whatsapp.net', id: 'M1' },
                    message: { conversation: 'oi' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    }).finish()
    assert.ok(
        validConversation.length < 128,
        'fixture must stay below 128 bytes for the single-byte length framing below'
    )
    const corruptRecord = new Uint8Array([0x12, 0x01, 0x0d])
    const blob = new Uint8Array(2 + 2 + validConversation.length + 2 + corruptRecord.length)
    blob.set([0x08, 0x02], 0)
    blob.set([0x12, validConversation.length], 2)
    blob.set(validConversation, 4)
    blob.set([0x12, corruptRecord.length], 4 + validConversation.length)
    blob.set(corruptRecord, 6 + validConversation.length)
    const gzipped = toBytesView(await promisify(gzip)(blob))

    let rejectedWriteSettled = false
    const threadJids: string[] = []
    const deps = {
        logger: createNoopLogger(),
        mediaTransfer: null as never,
        writeBehind: {
            ownHistoryOperation: (start: () => Promise<void>) => start(),
            historyOperationsSettled: () => false,
            persistContactAsync: async () => undefined,
            persistThreadAsync: (input: { readonly jid: string }) => {
                threadJids.push(input.jid)
                return Promise.reject(new Error('write failed')).catch((error: unknown) => {
                    rejectedWriteSettled = true
                    throw error
                })
            },
            persistMessageAsync: async () => undefined
        } as never,
        emitEvent: () => {
            throw new Error('event must not be emitted on abort')
        },
        onProcessed: async () => {
            throw new Error('chunk must not be acked on abort')
        }
    }

    await assert.rejects(
        () =>
            processHistorySyncNotification(deps as never, {
                syncType: proto.Message.HistorySyncType.RECENT,
                initialHistBootstrapInlinePayload: gzipped
            }),
        /invalid|index out of range|protobuf/i
    )
    assert.equal(rejectedWriteSettled, true, 'pending write rejection must be consumed')
    assert.deepEqual(
        threadJids,
        ['5511111111111@s.whatsapp.net'],
        'the conversation before the corrupt record must have been written'
    )
})

test('history sync field numbers match the generated encoder', () => {
    assert.deepEqual(
        {
            SYNC_TYPE: encodedFieldNumber(proto.HistorySync, { syncType: 1 }),
            CONVERSATIONS: encodedFieldNumber(proto.HistorySync, { conversations: [{ id: 'x' }] }),
            CHUNK_ORDER: encodedFieldNumber(proto.HistorySync, { chunkOrder: 1 }),
            PROGRESS: encodedFieldNumber(proto.HistorySync, { progress: 1 }),
            PUSHNAMES: encodedFieldNumber(proto.HistorySync, { pushnames: [{ id: 'x' }] }),
            PHONE_NUMBER_TO_LID_MAPPINGS: encodedFieldNumber(proto.HistorySync, {
                phoneNumberToLidMappings: [{ pnJid: 'x' }]
            }),
            NCT_SALT: encodedFieldNumber(proto.HistorySync, { nctSalt: new Uint8Array([1]) }),
            INLINE_CONTACTS: encodedFieldNumber(proto.HistorySync, {
                inlineContacts: [{ pnJid: 'x' }]
            })
        },
        { ...HISTORY_SYNC_FIELDS }
    )

    assert.deepEqual(
        {
            ID: encodedFieldNumber(proto.Conversation, { id: 'x' }),
            MESSAGES: encodedFieldNumber(proto.Conversation, { messages: [{}] })
        },
        { ...CONVERSATION_FIELDS }
    )
})

test('destroying the inflated stream tears down the decryption source', async () => {
    const plaintext = new Readable({ read: () => undefined })
    const blob = await openHistoryBlobStream(
        {
            downloadAndDecryptStream: async () => ({
                plaintext,
                metadata: Promise.resolve(null)
            })
        } as never,
        {
            directPath: '/history',
            mediaKey: new Uint8Array(32),
            fileSha256: new Uint8Array(32),
            fileEncSha256: new Uint8Array(32)
        },
        'history',
        'history sync'
    )

    assert.equal(plaintext.destroyed, false)
    blob.inflated.destroy(new Error('consumer gave up'))
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(plaintext.destroyed, true, 'the source must not keep draining after an abort')
})

test('history sync bounds the messages parked before their thread jid', async () => {
    const encodeVarint = (value: number): number[] => {
        const out: number[] = []
        let rest = value
        while (rest > 0x7f) {
            out.push((rest & 0x7f) | 0x80)
            rest = Math.floor(rest / 128)
        }
        out.push(rest)
        return out
    }

    const messages: Proto.IHistorySyncMsg[] = []
    for (let index = 0; index < 1_200; index += 1) {
        messages.push({
            message: {
                key: { remoteJid: '5511444444444@s.whatsapp.net', id: `P${index}` },
                message: { conversation: 'parked' },
                messageTimestamp: 1_722_000_000 + index
            }
        })
    }
    const messagesPart = proto.Conversation.encode({ messages }).finish()
    const idPart = proto.Conversation.encode({ id: '5511444444444@s.whatsapp.net' }).finish()
    const conversation = new Uint8Array(messagesPart.length + idPart.length)
    conversation.set(messagesPart, 0)
    conversation.set(idPart, messagesPart.length)
    const blob = new Uint8Array([0x12, ...encodeVarint(conversation.length), ...conversation])

    const payload = toBytesView(await gzipAsync(blob))
    const { capture, deps } = createCapture()
    await assert.rejects(
        () =>
            processHistorySyncNotification(deps as never, {
                syncType: proto.Message.HistorySyncType.RECENT,
                initialHistBootstrapInlinePayload: payload
            }),
        /parked 1024 messages with no thread jid/
    )

    assert.deepEqual(capture.acked, [], 'an aborted chunk must stay unacked so it is resent')
    assert.deepEqual(capture.events, [], 'no chunk event may report a partial apply')
})

test('history sync counts only the messages it persisted', async () => {
    const { capture, deps } = createCapture()
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(
            await gzipAsync(
                proto.HistorySync.encode({
                    conversations: [
                        {
                            id: '5511222222222@s.whatsapp.net',
                            messages: [
                                {
                                    message: {
                                        key: {
                                            remoteJid: '5511222222222@s.whatsapp.net',
                                            id: 'KEPT'
                                        },
                                        message: { conversation: 'kept' },
                                        messageTimestamp: 1_722_000_000
                                    }
                                },
                                {
                                    message: {
                                        key: {
                                            remoteJid: '5511222222222@s.whatsapp.net',
                                            id: 'STUB'
                                        },
                                        messageStubType:
                                            proto.WebMessageInfo.StubType.GROUP_PARTICIPANT_ADD
                                    }
                                }
                            ]
                        }
                    ]
                }).finish()
            )
        )
    })

    const emitted = capture.events[0] as Record<string, number>
    assert.equal(capture.messages.length, 1)
    assert.equal(
        emitted.messagesCount,
        capture.messages.length,
        'the reported count must match what was written'
    )
})

async function runConversation(
    conversation: proto.IConversation,
    meJid?: string
): Promise<Capture> {
    const { capture, deps } = createCapture(meJid)
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(
            await gzipAsync(proto.HistorySync.encode({ conversations: [conversation] }).finish())
        )
    })
    return capture
}

test('history sync reads a group sender from the top-level WebMessageInfo participant', async () => {
    const capture = await runConversation({
        id: '120363000000000000@g.us',
        messages: [
            {
                message: {
                    key: { remoteJid: '120363000000000000@g.us', id: 'G1' },
                    participant: '5511111111111@s.whatsapp.net',
                    message: { conversation: 'Good copy' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    })

    assert.equal(capture.messages.length, 1)
    assert.equal(capture.messages[0].senderJid, '5511111111111@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, '5511111111111@s.whatsapp.net')
})

test('history sync prefers the top-level participant over key.participant', async () => {
    const capture = await runConversation({
        id: '120363000000000000@g.us',
        messages: [
            {
                message: {
                    key: {
                        remoteJid: '120363000000000000@g.us',
                        id: 'G2',
                        participant: '5522222222222@s.whatsapp.net'
                    },
                    participant: '222@lid',
                    message: { conversation: 'oi' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    })

    assert.equal(capture.messages[0].senderJid, '222@lid')
    assert.equal(capture.messages[0].participantJid, '222@lid')
})

test('history sync still reads a group sender carried only on key.participant', async () => {
    const capture = await runConversation({
        id: '120363000000000000@g.us',
        messages: [
            {
                message: {
                    key: {
                        remoteJid: '120363000000000000@g.us',
                        id: 'G3',
                        participant: '5533333333333@s.whatsapp.net'
                    },
                    message: { conversation: 'oi' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    })

    assert.equal(capture.messages[0].senderJid, '5533333333333@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, '5533333333333@s.whatsapp.net')
})

test('history sync attributes a self-sent group message with no participant to this account', async () => {
    const capture = await runConversation(
        {
            id: '120363000000000000@g.us',
            messages: [
                {
                    message: {
                        key: { remoteJid: '120363000000000000@g.us', id: 'G4', fromMe: true },
                        message: { conversation: 'mine' },
                        messageTimestamp: 1_722_000_000
                    }
                }
            ]
        },
        '5599999999999:7@s.whatsapp.net'
    )

    assert.equal(capture.messages[0].senderJid, '5599999999999@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, '5599999999999@s.whatsapp.net')
})

test('history sync prefers originalSelfAuthorUserJidString over the current account jid', async () => {
    const capture = await runConversation(
        {
            id: '120363000000000000@g.us',
            messages: [
                {
                    message: {
                        key: { remoteJid: '120363000000000000@g.us', id: 'G5', fromMe: true },
                        originalSelfAuthorUserJidString: '5588888888888@s.whatsapp.net',
                        message: { conversation: 'mine' },
                        messageTimestamp: 1_722_000_000
                    }
                }
            ]
        },
        '5599999999999@s.whatsapp.net'
    )

    assert.equal(capture.messages[0].senderJid, '5588888888888@s.whatsapp.net')
})

test('history sync falls back to the thread jid for a 1:1 sender and leaves participantJid unset', async () => {
    const capture = await runConversation({
        id: '5511444444444@s.whatsapp.net',
        messages: [
            {
                message: {
                    key: { remoteJid: '5511444444444@s.whatsapp.net', id: 'D1' },
                    message: { conversation: 'oi' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    })

    assert.equal(capture.messages[0].senderJid, '5511444444444@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, undefined)
})

test('history sync keeps the sender on messages parked before their thread jid', async () => {
    const messagesPart = proto.Conversation.encode({
        messages: [
            {
                message: {
                    key: { remoteJid: '120363000000000000@g.us', id: 'P1' },
                    participant: '5511111111111@s.whatsapp.net',
                    message: { conversation: 'parked' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    }).finish()
    const idPart = proto.Conversation.encode({ id: '120363000000000000@g.us' }).finish()
    const conversation = new Uint8Array(messagesPart.length + idPart.length)
    conversation.set(messagesPart, 0)
    conversation.set(idPart, messagesPart.length)
    const blob = new Uint8Array([0x12, conversation.length & 0x7f, ...conversation])
    assert.ok(conversation.length < 128, 'the probe conversation must fit a one-byte length')

    const { capture, deps } = createCapture()
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(blob))
    })

    assert.equal(capture.messages.length, 1)
    assert.equal(capture.messages[0].threadJid, '120363000000000000@g.us')
    assert.equal(capture.messages[0].senderJid, '5511111111111@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, '5511111111111@s.whatsapp.net')
})

test('history sync resolves a parked self-sent group message once the thread jid lands', async () => {
    const messagesPart = proto.Conversation.encode({
        messages: [
            {
                message: {
                    key: { id: 'PSELF', fromMe: true },
                    message: { conversation: 'parked and mine' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    }).finish()
    const idPart = proto.Conversation.encode({ id: '120363000000000000@g.us' }).finish()
    const conversation = new Uint8Array(messagesPart.length + idPart.length)
    conversation.set(messagesPart, 0)
    conversation.set(idPart, messagesPart.length)
    assert.ok(conversation.length < 128, 'the probe conversation must fit a one-byte length')
    const blob = new Uint8Array([0x12, conversation.length, ...conversation])

    const { capture, deps } = createCapture('5599999999999:7@s.whatsapp.net')
    await processHistorySyncNotification(deps as never, {
        syncType: proto.Message.HistorySyncType.RECENT,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(blob))
    })

    assert.equal(capture.messages.length, 1)
    assert.equal(capture.messages[0].senderJid, '5599999999999@s.whatsapp.net')
    assert.equal(capture.messages[0].participantJid, '5599999999999@s.whatsapp.net')
})

test('history sync leaves an unresolved group author empty instead of naming the group', async () => {
    const capture = await runConversation({
        id: '120363000000000000@g.us',
        messages: [
            {
                message: {
                    key: { remoteJid: '120363000000000000@g.us', id: 'GHOST' },
                    message: { conversation: 'no author anywhere' },
                    messageTimestamp: 1_722_000_000
                }
            }
        ]
    })

    assert.equal(capture.messages.length, 1)
    assert.equal(capture.messages[0].senderJid, undefined)
    assert.equal(capture.messages[0].participantJid, undefined)
})

const DISPOSITION_LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 65_536,
    maxBatchBytes: 65_536,
    maxMessagesPerBatch: 32,
    maxSourceRecordsPerBatch: 1_000,
    timeoutMs: 2_000
}

function acceptedRootFields(): Uint8Array {
    return concatenate(
        encodeBytesField(
            8,
            proto.GlobalSettings.encode({
                avatarUserSettings: { password: 'synthetic-settings-secret' },
                chatDbLidMigrationTimestamp: 1
            }).finish()
        ),
        encodeBytesField(9, new TextEncoder().encode('synthetic-thread-secret')),
        encodeVarintField(10, 0xffff_ffff),
        encodeBytesField(
            12,
            proto.PastParticipants.encode({
                groupJid: 'synthetic-group@g.us',
                pastParticipants: [{ userJid: 'synthetic-participant@s.whatsapp.net', leaveTs: 1 }]
            }).finish()
        ),
        encodeBytesField(13, proto.CallLogRecord.encode({ callId: 'synthetic-call-id' }).finish())
    )
}

function dispositionHistory(
    options: { parked?: boolean; bad?: Uint8Array; content?: Uint8Array } = {}
): Uint8Array {
    const extended = concatenate(
        encodeStringField(1, 'exact synthetic text'),
        encodeStringField(4, 'synthetic-excluded-url-outer'),
        encodeBytesField(
            17,
            encodeBytesField(
                3,
                encodeBytesField(
                    6,
                    concatenate(
                        encodeStringField(1, 'synthetic-quoted-body'),
                        encodeStringField(4, 'synthetic-excluded-url-quoted')
                    )
                )
            )
        )
    )
    const message = options.content ?? encodeBytesField(6, extended)
    const key = proto.MessageKey.encode({
        remoteJid: 'synthetic-peer@s.whatsapp.net',
        id: 'synthetic-message-id',
        fromMe: false
    }).finish()
    const record = concatenate(
        encodeBytesField(
            1,
            concatenate(
                encodeBytesField(1, key),
                encodeBytesField(2, message),
                encodeVarintField(3, 123)
            )
        ),
        encodeVarintField(2, 77)
    )
    const id = encodeStringField(1, 'synthetic-peer@s.whatsapp.net')
    const messages = encodeBytesField(2, record)
    const conversation = options.parked ? concatenate(messages, id) : concatenate(id, messages)
    return concatenate(
        encodeVarintField(1, 3),
        acceptedRootFields(),
        encodeBytesField(2, conversation),
        options.bad ?? new Uint8Array()
    )
}

async function dispositionProcess(
    payload: Uint8Array,
    onBatch?: (batch: WaHistorySyncBatchV1) => Promise<void>,
    enabled = true
) {
    const { capture, deps } = createCapture()
    let batch: WaHistorySyncBatchV1 | undefined
    await processHistorySyncNotification(
        deps as never,
        {
            syncType: 3,
            initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(payload))
        },
        enabled
            ? {
                  limits: DISPOSITION_LIMITS,
                  signal: new AbortController().signal,
                  onBatch: async (current) => {
                      batch = current
                      await onBatch?.(current)
                  }
              }
            : undefined
    )
    return { capture, batch }
}

test('PROC-01/03/09 all exclusions coexist; canonical facts, parking and private archive stay exact', async () => {
    for (const parked of [false, true]) {
        const { capture, batch } = await dispositionProcess(dispositionHistory({ parked }))
        assert.ok(batch)
        const exclusions = batch.sourceFamilyDispositions.filter(
            (row) => row.disposition === 'EXCLUDED_BY_ACCEPTED_SCOPE'
        )
        assert.deepEqual(
            exclusions.map((row) => [row.scope, row.fieldNumber, row.count]),
            [
                ['MESSAGE_EXTENDED_TEXT', 4, 1],
                ['QUOTED_MESSAGE_EXTENDED_TEXT', 4, 1],
                ['ROOT', 8, 1],
                ['ROOT', 9, 1],
                ['ROOT', 10, 1],
                ['ROOT', 12, 1],
                ['ROOT', 13, 1]
            ]
        )
        assert.deepEqual(batch.messageObservations[0], {
            ordinal: 1,
            conversationOrdinal: 1,
            chatKind: 'DIRECT',
            disposition: 'IMPORT_DIRECT',
            externalMessageId: 'synthetic-message-id',
            conversationAddress: 'synthetic-peer@s.whatsapp.net',
            messageRemoteJid: 'synthetic-peer@s.whatsapp.net',
            fromMe: false,
            timestampSeconds: '123',
            msgOrderId: '77',
            content: { kind: 'TEXT', text: 'exact synthetic text' }
        })
        assert.equal(capture.messages.length, 1)
        for (const value of [
            JSON.stringify(batch),
            JSON.stringify(capture.events),
            JSON.stringify(capture.threads),
            new TextDecoder().decode(capture.messages[0].messageBytes)
        ]) {
            for (const forbidden of [
                'synthetic-settings-secret',
                'synthetic-thread-secret',
                'synthetic-participant',
                'synthetic-group',
                'synthetic-call-id',
                'synthetic-excluded-url'
            ])
                assert.equal(value.includes(forbidden), false)
        }
        const archived = proto.Message.decode(capture.messages[0].messageBytes!)
        assert.equal(archived.extendedTextMessage!.text, 'exact synthetic text')
        assert.equal(archived.extendedTextMessage!.$unknowns?.length ?? 0, 0)
        assert.equal(
            archived.extendedTextMessage!.contextInfo!.quotedMessage!.extendedTextMessage!.$unknowns
                ?.length ?? 0,
            0
        )
        assert.deepEqual(capture.acked, [3])
    }
})

test('PROC-02 repeated root occurrences and field order do not collapse physical counts', async () => {
    const payload = concatenate(dispositionHistory(), acceptedRootFields(), acceptedRootFields())
    const { batch } = await dispositionProcess(payload)
    assert.ok(batch)
    assert.ok(
        batch.sourceFamilyDispositions
            .filter((row) => row.scope === 'ROOT' && [8, 9, 10, 12, 13].includes(row.fieldNumber))
            .every((row) => row.count === 3)
    )
})

test('PROC-04/05 unrelated unknown or malformed accepted field blocks the whole batch before writes/callback', async () => {
    for (const bad of [
        encodeBytesField(11, new Uint8Array()),
        encodeVarintField(10, 4_294_967_296),
        encodeBytesField(8, encodeVarintField(999, 1)),
        encodeBytesField(13, encodeBytesField(14, encodeVarintField(99, 1)))
    ]) {
        let called = false
        await assert.rejects(
            dispositionProcess(dispositionHistory({ bad }), async () => {
                called = true
            })
        )
        assert.equal(called, false)
    }
    await assert.rejects(
        dispositionProcess(
            dispositionHistory({
                content: encodeBytesField(
                    6,
                    concatenate(
                        encodeStringField(1, 'synthetic-body'),
                        encodeStringField(4, 'synthetic-excluded-url'),
                        encodeVarintField(99, 1)
                    )
                )
            })
        ),
        /unknown_extended_text_fields/
    )
})

test('PROC-06 field 4 never supplies body or repairs missing direct identity', async () => {
    const { batch } = await dispositionProcess(
        dispositionHistory({ content: encodeBytesField(6, encodeStringField(4, 'synthetic-url')) })
    )
    assert.equal(batch!.messageObservations[0].content.kind, 'UNSUPPORTED')
    const badRecord = encodeBytesField(
        1,
        proto.WebMessageInfo.encode({
            key: { remoteJid: 'synthetic-peer@s.whatsapp.net', fromMe: false },
            message: { conversation: 'synthetic-body' }
        }).finish()
    )
    const payload = concatenate(
        encodeVarintField(1, 3),
        acceptedRootFields(),
        encodeBytesField(
            2,
            concatenate(
                encodeStringField(1, 'synthetic-peer@s.whatsapp.net'),
                encodeBytesField(2, badRecord)
            )
        )
    )
    await assert.rejects(dispositionProcess(payload), /missing_required_identity/)
})

test('PROC-07 root calls do not remove independently classified call-related observations', async () => {
    const callMessage = proto.Message.encode({ callLogMesssage: {} }).finish()
    // This pinned misspelled content key was already unclassified; do not expand its support.
    await assert.rejects(
        dispositionProcess(dispositionHistory({ content: callMessage })),
        /content_unclassified/
    )
    const record = proto.HistorySyncMsg.encode({
        message: {
            key: {
                remoteJid: 'synthetic-peer@s.whatsapp.net',
                id: 'synthetic-message-id',
                fromMe: false
            },
            message: { conversation: 'synthetic-call-stub-text' },
            messageStubType: proto.WebMessageInfo.StubType.CALL_MISSED_VOICE
        }
    }).finish()
    const payload = concatenate(
        encodeVarintField(1, 3),
        acceptedRootFields(),
        encodeBytesField(
            2,
            concatenate(
                encodeStringField(1, 'synthetic-peer@s.whatsapp.net'),
                encodeBytesField(2, record)
            )
        )
    )
    const { batch } = await dispositionProcess(payload)
    assert.equal(batch!.messageObservations.length, 1)
    assert.equal(
        batch!.messageObservations[0].messageStubType,
        proto.WebMessageInfo.StubType.CALL_MISSED_VOICE
    )
    assert.equal(batch!.messageObservations[0].externalMessageId, 'synthetic-message-id')
})

test('PROC-08 excluded roots leave independently supported conversation/private handlers unchanged', async () => {
    const payload = concatenate(dispositionHistory(), encodeBytesField(19, new Uint8Array([1, 2])))
    const { capture, batch } = await dispositionProcess(payload)
    assert.deepEqual(capture.salts, [new Uint8Array([1, 2])])
    assert.equal(batch!.messageObservations[0].timestampSeconds, '123')
    assert.equal(batch!.messageObservations[0].msgOrderId, '77')
    assert.deepEqual(capture.tokens, [])
})

test('PROC-09 disabled boundary preserves legacy encoded unknown URL bytes unchanged', async () => {
    const { capture, batch } = await dispositionProcess(dispositionHistory(), undefined, false)
    assert.equal(batch, undefined)
    const archived = proto.Message.decode(capture.messages[0].messageBytes!)
    assert.equal(archived.extendedTextMessage!.$unknowns?.length, 1)
    assert.equal(
        archived.extendedTextMessage!.contextInfo!.quotedMessage!.extendedTextMessage!.$unknowns
            ?.length,
        1
    )
    assert.equal(
        new TextDecoder()
            .decode(capture.messages[0].messageBytes)
            .includes('synthetic-excluded-url-outer'),
        true
    )
})

test('PROC-10 rejection/abort before callback success withholds processing and deferred writes', async () => {
    const { capture, deps } = createCapture()
    const input = {
        syncType: 3,
        initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(dispositionHistory()))
    }
    await assert.rejects(
        processHistorySyncNotification(deps as never, input, {
            limits: DISPOSITION_LIMITS,
            signal: new AbortController().signal,
            onBatch: async () => {
                throw new Error('synthetic-consumer-failure')
            }
        }),
        /synthetic-consumer-failure/
    )
    assert.deepEqual(capture.messages, [])
    assert.deepEqual(capture.acked, [])
})

for (const tag of [8, 9, 10, 12, 13]) {
    test(`PROC matrix root ${tag}: standalone, repeated, mixed DIRECT and any unrelated blocker`, async () => {
        const valid =
            tag === 10
                ? encodeVarintField(10, 0)
                : encodeBytesField(
                      tag,
                      tag === 12 ? encodeStringField(1, 'synthetic-group@g.us') : new Uint8Array()
                  )
        const base = encodeVarintField(1, 3)
        for (const payload of [
            concatenate(base, valid),
            concatenate(base, valid, valid),
            concatenate(dispositionHistory(), valid)
        ]) {
            const { batch } = await dispositionProcess(payload)
            const row = batch!.sourceFamilyDispositions.find(
                (item) => item.scope === 'ROOT' && item.fieldNumber === tag
            )!
            assert.equal(row.disposition, 'EXCLUDED_BY_ACCEPTED_SCOPE')
            assert.ok(row.count === 1 || row.count === 2)
        }
        for (const payload of [
            concatenate(base, valid, encodeBytesField(11, new Uint8Array())),
            concatenate(base, encodeBytesField(11, new Uint8Array()), valid),
            concatenate(dispositionHistory(), valid, encodeBytesField(8, encodeVarintField(999, 1)))
        ]) {
            let called = false
            await assert.rejects(
                dispositionProcess(payload, async () => {
                    called = true
                })
            )
            assert.equal(called, false)
        }
    })
}

test('PROC nested work is charged exactly once across parked and nonparked processing', async () => {
    for (const parked of [false, true]) {
        const payload = dispositionHistory({ parked })
        const input = {
            syncType: 3,
            initialHistBootstrapInlinePayload: toBytesView(await gzipAsync(payload))
        }
        let firstPassing = 0
        for (let maxWork = 1; maxWork <= 100; maxWork += 1) {
            const { capture, deps } = createCapture()
            try {
                await processHistorySyncNotification(deps as never, input, {
                    limits: { ...DISPOSITION_LIMITS, maxSourceRecordsPerBatch: maxWork },
                    signal: new AbortController().signal,
                    onBatch: async () => undefined
                })
                firstPassing = maxWork
                break
            } catch (error) {
                assert.match((error as Error).message, /source-record limit/)
                assert.deepEqual(capture.messages, [])
                assert.deepEqual(capture.acked, [])
            }
        }
        // 8 stream FIELD events + 8 excluded-root descendants + 16 record descendants.
        // Conversation ENTER/LEAVE, parked replay and projection add no FIELD work.
        assert.equal(firstPassing, 32)
    }
})
