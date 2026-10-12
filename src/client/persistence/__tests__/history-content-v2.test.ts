import assert from 'node:assert/strict'
import test from 'node:test'
import { inspect } from 'node:util'
import { gzipSync } from 'node:zlib'

import { projectHistoryContentV2 } from '@client/persistence/history-content-v2'
import { processHistorySyncNotification } from '@client/persistence/history-sync'
import { HistorySyncBoundaryWorker } from '@client/persistence/HistorySyncBoundaryWorker'
import { HistorySyncStreamV2 } from '@client/persistence/HistorySyncStreamV2'
import { WaHistorySyncMediaSourceV2 } from '@client/persistence/WaHistorySyncMediaSourceV2'
import type {
    WaClientOptions,
    WaHistorySyncBatchV1,
    WaHistorySyncResourceLimits,
    WaHistorySyncStreamEventV2,
    WaHistorySyncStreamLimits
} from '@client/types'
import { resolveWaClientBase } from '@client/WaClientFactory'
import { createNoopLogger } from '@infra/log/types'
import type { WaResolvedMediaPayload } from '@message/encode/media-payload'
import { proto, type Proto } from '@proto'
import { concatBytes, TEXT_ENCODER } from '@util/bytes'

const ADDRESS = 'synthetic@s.whatsapp.net'
const RESOURCES: WaHistorySyncResourceLimits = {
    maxRecordBytes: 8_192,
    maxConversationHeaderBytes: 4_096,
    maxParkedBytes: 8_192,
    maxRetainedPrivateBytes: 32_768,
    maxRetainedNotificationBytes: 16_384,
    maxThumbnailBytes: 1_024
}
const STREAM_LIMITS: WaHistorySyncStreamLimits = {
    maxMessagesPerChunk: 2,
    maxChunkBytes: 8_192,
    maxParkedMessages: 2,
    resources: RESOURCES
}
const LIMITS = {
    maxConcurrentBatches: 1,
    maxQueuedBatches: 1,
    maxQueuedBytes: 65_536,
    maxBatchBytes: 65_536,
    maxMessagesPerBatch: 128,
    maxSourceRecordsPerBatch: 8_192,
    timeoutMs: 2_000
}

function mediaFields(mimetype = 'image/jpeg') {
    return {
        mimetype,
        directPath: '/v/synthetic.enc?test=1',
        mediaKey: new Uint8Array(32).fill(1),
        fileSha256: new Uint8Array(32).fill(2),
        fileEncSha256: new Uint8Array(32).fill(3),
        caption: 'synthetic caption',
        width: 32,
        height: 16,
        seconds: 5
    }
}

const MEDIA_CASES: readonly [string, Proto.IMessage, WaResolvedMediaPayload['mediaType']][] = [
    ['IMAGE', { imageMessage: mediaFields() }, 'image'],
    ['VIDEO', { videoMessage: mediaFields('video/mp4') }, 'video'],
    ['VIDEO', { videoMessage: { ...mediaFields('video/mp4'), gifPlayback: true } }, 'gif'],
    ['AUDIO', { audioMessage: mediaFields('audio/mpeg') }, 'audio'],
    [
        'VOICE_NOTE',
        { audioMessage: { ...mediaFields('audio/ogg; codecs=opus'), ptt: true } },
        'ptt'
    ],
    [
        'DOCUMENT',
        { documentMessage: { ...mediaFields('application/pdf'), fileName: 'safe.pdf' } },
        'document'
    ],
    ['STICKER', { stickerMessage: { ...mediaFields('image/webp'), isAnimated: true } }, 'sticker']
]

for (const [kind, message, cryptoType] of MEDIA_CASES) {
    test(`rich ${kind}/${cryptoType} projects supported metadata without retrieval or receipt authority`, () => {
        const decoded = proto.Message.decode(proto.Message.encode(message).finish())
        const row = projectHistoryContentV2(decoded, RESOURCES.maxThumbnailBytes)
        assert.equal(row.content.kind, kind)
        assert.ok('media' in row.content)
        assert.equal(row.content.media.retrievalState, 'DESCRIPTOR_AVAILABLE')
        assert.equal(row.content.media.thumbnailState, 'ABSENT')
        assert.equal(row.mediaSource?.getTransferInput().mediaType, cryptoType)
        assert.equal('receipt' in row, false)
        assert.equal('status' in row, false)
        assert.equal('url' in row.content.media, false)
        assert.equal('directPath' in row.content.media, false)
        assert.equal('mediaKey' in row.content.media, false)
        if (cryptoType === 'gif') assert.equal(row.content.media.gifPlayback, true)
        if (cryptoType === 'ptt') {
            assert.equal(row.content.media.ptt, true)
            assert.equal(row.content.media.mimetype, 'audio/ogg')
        }
    })
}

test('protected handle masks inspection, rejects JSON and detaches source and export mutations', () => {
    const backing = new Uint8Array(1_000_000).fill(1)
    const input: WaResolvedMediaPayload = {
        mediaType: 'image',
        directPath: '/synthetic.enc',
        mediaKey: backing.subarray(0, 32),
        fileSha256: backing.subarray(32, 64),
        fileEncSha256: backing.subarray(64, 96)
    }
    const source = new WaHistorySyncMediaSourceV2(input)
    backing.fill(9)
    const exported = source.getTransferInput()
    assert.equal(exported.mediaKey[0], 1)
    assert.equal(exported.mediaKey.buffer.byteLength, 32)
    assert.equal(exported.fileSha256!.buffer.byteLength, 32)
    exported.mediaKey.fill(8)
    assert.equal(source.getTransferInput().mediaKey[0], 1)
    assert.equal(Object.isFrozen(source), true)
    assert.deepEqual(Object.keys(source), [])
    assert.equal(inspect(source), 'WaHistorySyncMediaSourceV2 { PROTECTED }')
    assert.throws(() => JSON.stringify({ source }), /protected persistence/)
    assert.ok(source.getRetainedByteLength() < 1_024)
})

test('relative media descriptor rejects external hosts, traversal, malformed keys and encoded separators', () => {
    for (const directPath of [
        'https://invalid.test/file',
        '//invalid.test/file',
        '/a/../b',
        '/a/./b',
        '/%2e%2e/b',
        '/%252e%252e/b',
        '/a%2fb',
        '/a%5cb',
        '/a\\b',
        '/a%00b',
        '/a%20b',
        '/a#fragment',
        '/%',
        '/x'.repeat(3_000)
    ]) {
        const row = projectHistoryContentV2(
            { imageMessage: { ...mediaFields(), directPath } },
            1_024
        )
        assert.ok('media' in row.content)
        assert.equal(row.content.media.retrievalState, 'UNAVAILABLE')
        assert.equal(row.content.media.retrievalReason, 'INVALID_DESCRIPTOR')
        assert.equal(row.mediaSource, undefined)
    }
    for (const key of ['mediaKey', 'fileSha256', 'fileEncSha256'] as const) {
        for (const bytes of [undefined, new Uint8Array(31), new Uint8Array(33)]) {
            const row = projectHistoryContentV2(
                { imageMessage: { ...mediaFields(), [key]: bytes } },
                1_024
            )
            assert.ok('media' in row.content)
            assert.equal(row.content.media.retrievalState, 'UNAVAILABLE')
            assert.equal(row.mediaSource, undefined)
        }
    }
})

test('unsafe/absent MIME and unavailable media stay visible metadata-only; URL is not a fallback', () => {
    for (const mimetype of [
        undefined,
        'image/svg+xml',
        'text/html',
        'image/jpeg\r\nunsafe',
        'x'.repeat(256)
    ]) {
        const row = projectHistoryContentV2({ imageMessage: { ...mediaFields(), mimetype } }, 1_024)
        assert.equal(row.content.kind, 'IMAGE')
        assert.ok('media' in row.content)
        assert.equal(row.content.media.retrievalReason, 'UNSAFE_MIME')
        assert.equal(row.content.media.mimetype, undefined)
        assert.equal(row.mediaSource, undefined)
    }
    const row = projectHistoryContentV2(
        {
            imageMessage: {
                mimetype: 'image/jpeg',
                caption: 'synthetic',
                url: 'https://invalid.test/ignored'
            }
        },
        1_024
    )
    assert.ok('media' in row.content)
    assert.equal(row.content.media.caption, 'synthetic')
    assert.equal(row.content.media.retrievalReason, 'MISSING_DESCRIPTOR')
})

test('media length preserves exact uint64 and rejects rounded or out-of-range inputs', () => {
    for (const length of ['9007199254740993', '18446744073709551615', '0']) {
        const bits = BigInt(length)
        const decoded = proto.Message.decode(
            proto.Message.encode({
                imageMessage: {
                    ...mediaFields(),
                    fileLength: {
                        low: Number(bits & 0xffff_ffffn),
                        high: Number(bits >> 32n),
                        unsigned: true,
                        toNumber: () => Number(bits)
                    }
                }
            }).finish()
        )
        const row = projectHistoryContentV2(decoded, 1_024)
        assert.ok('media' in row.content)
        assert.equal(row.content.media.fileLength, length)
    }
    for (const fileLength of [
        -1,
        NaN,
        Infinity,
        Number.MAX_SAFE_INTEGER + 1,
        { toString: () => '18446744073709551616' }
    ]) {
        const row = projectHistoryContentV2(
            { imageMessage: { ...mediaFields(), fileLength } } as Proto.IMessage,
            1_024
        )
        assert.ok('media' in row.content)
        assert.equal(row.content.media.fileLength, undefined)
    }
})

test('thumbnail projection is independently bounded and admits only a JPEG signature', () => {
    for (const [thumbnail, max, expected] of [
        [undefined, 4, 'ABSENT'],
        [new Uint8Array([0xff, 0xd8, 0xff, 0]), 4, 'AVAILABLE'],
        [new Uint8Array([0xff, 0xd8, 0xff, 0]), 3, 'OMITTED_LIMIT'],
        [new Uint8Array([0x89, 0x50, 0x4e, 0x47]), 4, 'UNSUPPORTED_FORMAT']
    ] as const) {
        const row = projectHistoryContentV2(
            { imageMessage: { ...mediaFields(), jpegThumbnail: thumbnail } },
            max
        )
        assert.ok('media' in row.content)
        assert.equal(row.content.media.thumbnailState, expected)
        assert.equal(
            row.content.media.thumbnail?.mimetype,
            expected === 'AVAILABLE' ? 'image/jpeg' : undefined
        )
    }
})

test('document filenames and numeric metadata are conservative and bounded', () => {
    for (const [fileName, expected] of [
        ['C:\\folder\\..\\safe.pdf', 'safe.pdf'],
        ['../../safe.pdf', 'safe.pdf'],
        ['..', 'attachment'],
        ['CON.pdf', '_CON.pdf'],
        ['name\u202etxt.pdf', 'name_txt.pdf'],
        ['bad\u0000<>.pdf', 'bad___.pdf'],
        ['x'.repeat(250), 'x'.repeat(200)]
    ]) {
        const row = projectHistoryContentV2(
            { documentMessage: { ...mediaFields('application/pdf'), fileName } },
            1_024
        )
        assert.ok('media' in row.content)
        assert.equal(row.content.media.fileName, expected)
    }
    const row = projectHistoryContentV2(
        {
            videoMessage: { ...mediaFields('video/mp4'), width: -1, height: 1.5, seconds: Infinity }
        },
        1_024
    )
    assert.ok('media' in row.content)
    assert.equal(row.content.media.width, undefined)
    assert.equal(row.content.media.height, undefined)
    assert.equal(row.content.media.seconds, undefined)
})

test('text, quoted and forwarded facts preserve source values without inventing direction', () => {
    const text = '<script>synthetic</script>\n😀 https://invalid.test'
    assert.deepEqual(projectHistoryContentV2({ conversation: text }, 1_024).content, {
        kind: 'TEXT',
        text
    })
    const row = projectHistoryContentV2(
        {
            extendedTextMessage: {
                text,
                contextInfo: {
                    isForwarded: false,
                    forwardingScore: 0,
                    stanzaId: 'synthetic-quoted',
                    participant: ADDRESS,
                    remoteJid: ADDRESS,
                    quotedMessage: { conversation: 'synthetic quote' }
                }
            }
        },
        1_024
    )
    assert.equal(row.context?.isForwarded, false)
    assert.equal(row.context?.forwardingScore, 0)
    assert.equal(row.context?.quote?.externalMessageId, 'synthetic-quoted')
    assert.equal(row.context?.quote?.preview?.kind, 'TEXT')
    assert.equal('fromMe' in row.context.quote, false)
    assert.equal('direction' in row.context.quote, false)
    const mediaQuote = projectHistoryContentV2(
        {
            extendedTextMessage: {
                text: 'synthetic',
                contextInfo: {
                    isForwarded: true,
                    forwardingScore: 2,
                    stanzaId: 'synthetic-media-quote',
                    quotedMessage: MEDIA_CASES[0][1]
                }
            }
        },
        1_024
    )
    assert.equal(mediaQuote.context?.quote?.preview?.kind, 'UNSUPPORTED')
    assert.equal(mediaQuote.mediaSource, undefined)
})

test('location ranges and contact cards are copied as source facts, without geocoding or identity merge', () => {
    const row = projectHistoryContentV2(
        {
            locationMessage: {
                degreesLatitude: 0,
                degreesLongitude: 0,
                name: 'synthetic',
                address: 'synthetic location'
            }
        },
        1_024
    )
    assert.equal(row.content.kind, 'LOCATION')
    for (const [latitude, longitude] of [
        [91, 0],
        [0, -181],
        [NaN, 0],
        [0, Infinity]
    ]) {
        assert.equal(
            projectHistoryContentV2(
                { locationMessage: { degreesLatitude: latitude, degreesLongitude: longitude } },
                1_024
            ).content.kind,
            'UNSUPPORTED'
        )
    }
    for (const message of [
        { contactMessage: { displayName: 'synthetic', vcard: 'synthetic vcard' } },
        {
            contactsArrayMessage: {
                contacts: [{ displayName: 'synthetic', vcard: 'synthetic vcard' }]
            }
        }
    ]) {
        const projected = projectHistoryContentV2(message, 1_024)
        assert.equal(projected.content.kind, 'CONTACT')
        assert.ok('contacts' in projected.content)
        assert.equal(projected.content.contacts.length, 1)
        assert.equal('identity' in projected, false)
    }
})

test('wrappers, view-once, reactions and protocol events remain closed placeholders', () => {
    for (const message of [
        { ephemeralMessage: { message: MEDIA_CASES[0][1] } },
        { viewOnceMessage: { message: MEDIA_CASES[0][1] } },
        { documentWithCaptionMessage: { message: MEDIA_CASES[5][1] } },
        { imageMessage: { ...mediaFields(), viewOnce: true } },
        { reactionMessage: { text: 'synthetic' } },
        { protocolMessage: { type: proto.Message.ProtocolMessage.Type.REVOKE } },
        {}
    ]) {
        const row = projectHistoryContentV2(message, 1_024)
        assert.equal(row.content.kind, 'UNSUPPORTED')
        assert.equal(row.mediaSource, undefined)
    }
})

test('ambiguous content and ambiguous quoted previews cannot pick an arbitrary payload', () => {
    const ambiguous = { conversation: 'synthetic', imageMessage: mediaFields() }
    assert.throws(() => projectHistoryContentV2(ambiguous, 1_024), /conflicting/)
    assert.throws(
        () =>
            projectHistoryContentV2(
                {
                    extendedTextMessage: {
                        text: 'synthetic',
                        contextInfo: { stanzaId: 'synthetic', quotedMessage: ambiguous }
                    }
                },
                1_024
            ),
        /conflicting/
    )
})

function fixture() {
    const calls: string[] = []
    const deps = {
        logger: createNoopLogger(),
        mediaTransfer: {
            downloadAndDecryptStream: async () => assert.fail('media fetch not authorized')
        } as never,
        writeBehind: {
            ownHistoryOperation: (start: () => Promise<void>) => start(),
            historyOperationsSettled: () => true,
            persistMessageAsync: async () => {
                calls.push('MESSAGE')
            },
            persistThreadAsync: async () => {
                calls.push('THREAD')
            },
            persistContactAsync: async () => {
                calls.push('CONTACT')
            }
        } as never,
        chatMetadataStore: {
            upsertChatMetadata: async () => {
                calls.push('EPHEMERAL')
            }
        } as never,
        onPrivacyTokens: async () => {
            calls.push('TOKENS')
        },
        onNctSalt: async (salt: Uint8Array) => {
            assert.equal(salt.buffer.byteLength, salt.byteLength)
            calls.push('SALT')
        },
        emitEvent: () => {
            calls.push('AGGREGATE')
        },
        onProcessed: async () => {
            calls.push('HIST_SYNC')
        }
    }
    return { calls, deps }
}

function historyPayload(
    messages: readonly Proto.IMessage[],
    extra: Proto.IHistorySync = {}
): Uint8Array {
    return proto.HistorySync.encode({
        syncType: 3,
        conversations: [
            {
                id: ADDRESS,
                messages: messages.map((message, index) => ({
                    message: {
                        key: { id: `synthetic-${index}`, remoteJid: ADDRESS, fromMe: false },
                        messageTimestamp: 1_700_000_000,
                        message
                    }
                }))
            }
        ],
        ...extra
    }).finish()
}

function options(events: WaHistorySyncStreamEventV2[], resources = RESOURCES) {
    return {
        signal: new AbortController().signal,
        limits: LIMITS,
        streamContent: 'RICH_V2' as const,
        privateMailbox: 'DISABLED' as const,
        streamLimits: { ...STREAM_LIMITS, resources },
        onStream: async (event: WaHistorySyncStreamEventV2) => {
            events.push(event)
        }
    }
}

test('actual validated parser delivers rich capture; disabled private mailbox leaves protocol-private hooks intact', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = historyPayload(
        MEDIA_CASES.map((row) => row[1]),
        {
            conversations: [
                {
                    id: ADDRESS,
                    name: 'synthetic',
                    pnJid: ADDRESS,
                    lidJid: 'synthetic@lid',
                    displayName: 'synthetic',
                    ephemeralExpiration: 86_400,
                    tcToken: new Uint8Array([1]),
                    messages: MEDIA_CASES.map((row, index) => ({
                        message: {
                            key: { id: `synthetic-${index}`, remoteJid: ADDRESS, fromMe: false },
                            message: row[1]
                        }
                    }))
                }
            ],
            pushnames: [{ id: ADDRESS, pushname: 'synthetic' }],
            phoneNumberToLidMappings: [{ pnJid: ADDRESS, lidJid: 'synthetic@lid' }],
            inlineContacts: [{ pnJid: ADDRESS, lidJid: 'synthetic@lid', fullName: 'synthetic' }],
            nctSalt: new Uint8Array([1])
        }
    )
    await processHistorySyncNotification(
        f.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        options(events)
    )
    const rows = events.flatMap((event) =>
        event.type === 'CHUNK' ? [...event.messageObservations] : []
    )
    assert.deepEqual(
        rows.map((row) => row.content.kind),
        MEDIA_CASES.map((row) => row[0])
    )
    assert.equal(events[0].type === 'BEGIN' && events[0].contentProfile, 'RICH_V2')
    const seal = events.at(-1)
    assert.ok(seal?.type === 'SEAL')
    assert.equal(seal.contentProfile, 'RICH_V2')
    assert.equal(seal.manifest.sourceHistoryCompleteness, 'UNKNOWN')
    assert.equal(f.calls.includes('MESSAGE'), false)
    assert.equal(f.calls.includes('THREAD'), false)
    assert.equal(f.calls.includes('CONTACT'), false)
    assert.ok(f.calls.includes('EPHEMERAL'))
    assert.ok(f.calls.includes('TOKENS'))
    assert.ok(f.calls.includes('SALT'))
    assert.equal(f.calls.at(-1), 'HIST_SYNC')
})

test('default v2 and v1 retain original TEXT/UNSUPPORTED projection and private message archive', async () => {
    const bytes = historyPayload([MEDIA_CASES[0][1], { conversation: 'synthetic' }])
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const config = options(events)
    await processHistorySyncNotification(
        f.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        {
            ...config,
            streamContent: undefined,
            privateMailbox: undefined
        }
    )
    assert.deepEqual(
        events.flatMap((event) =>
            event.type === 'CHUNK' ? event.messageObservations.map((row) => row.content.kind) : []
        ),
        ['UNSUPPORTED', 'TEXT']
    )
    assert.equal(events[0].type === 'BEGIN' && events[0].contentProfile, undefined)
    assert.equal(f.calls.filter((call) => call === 'MESSAGE').length, 2)
    const batches: WaHistorySyncBatchV1[] = []
    await processHistorySyncNotification(
        f.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        {
            signal: config.signal,
            limits: LIMITS,
            onBatch: async (batch) => {
                batches.push(batch)
            }
        }
    )
    assert.deepEqual(
        batches[0].messageObservations.map((row) => row.content.kind),
        ['UNSUPPORTED', 'TEXT']
    )
})

test('chunk byte budget includes protected handles without serializing their secret material', async () => {
    const projection = projectHistoryContentV2(MEDIA_CASES[0][1], 1_024)
    const row = {
        ordinal: 1,
        conversationOrdinal: 1,
        chatKind: 'DIRECT' as const,
        disposition: 'IMPORT_DIRECT' as const,
        ...projection
    }
    const { mediaSource, ...metadata } = row
    const charge =
        2 +
        TEXT_ENCODER.encode(JSON.stringify(metadata)).byteLength +
        mediaSource!.getRetainedByteLength()
    for (const [maxChunkBytes, succeeds] of [
        [charge, true],
        [charge - 1, false]
    ] as const) {
        const events: WaHistorySyncStreamEventV2[] = []
        const stream = new HistorySyncStreamV2(
            async (event) => {
                events.push(event)
            },
            { ...STREAM_LIMITS, maxChunkBytes },
            new AbortController().signal,
            'RICH_V2'
        )
        await stream.begin({ syncType: 3 })
        if (succeeds) await stream.append(row)
        else await assert.rejects(stream.append(row), /chunk byte limit/)
        await stream.abort()
    }
})

test('validated rich parser also captures text/context/location/cards and excludes non-DIRECT detail', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const messages: Proto.IMessage[] = [
        {
            extendedTextMessage: {
                text: 'synthetic',
                contextInfo: {
                    isForwarded: true,
                    forwardingScore: 1,
                    stanzaId: 'synthetic-quote',
                    quotedMessage: { conversation: 'synthetic quote' }
                }
            }
        },
        { locationMessage: { degreesLatitude: 0, degreesLongitude: 0 } },
        { contactMessage: { displayName: 'synthetic', vcard: 'synthetic vcard' } },
        {
            contactsArrayMessage: {
                contacts: [{ displayName: 'synthetic', vcard: 'synthetic vcard' }]
            }
        }
    ]
    const direct = proto.HistorySync.decode(historyPayload(messages)).conversations[0]
    const group = {
        id: 'synthetic@g.us',
        messages: [
            {
                message: {
                    key: { id: 'synthetic-group', remoteJid: 'synthetic@g.us', fromMe: false },
                    message: MEDIA_CASES[0][1]
                }
            }
        ]
    }
    await processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            initialHistBootstrapInlinePayload: gzipSync(
                historyPayload([], { conversations: [direct, group] })
            )
        },
        options(events)
    )
    const rows = events.flatMap((event) =>
        event.type === 'CHUNK' ? [...event.messageObservations] : []
    )
    assert.deepEqual(
        rows.map((row) => row.content.kind),
        ['TEXT', 'LOCATION', 'CONTACT', 'CONTACT', 'UNSUPPORTED']
    )
    assert.equal(rows[0].context?.isForwarded, true)
    assert.equal(rows[0].context?.quote?.preview?.kind, 'TEXT')
    assert.equal(rows[4].disposition, 'EXCLUDED_GROUP')
    assert.equal(rows[4].mediaSource, undefined)
    assert.equal(rows[4].context, undefined)
})

test('ambiguous rich content fails through the actual parser without SEAL or source advancement', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    const bytes = historyPayload([{ conversation: 'synthetic', imageMessage: mediaFields() }])
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
            options(events)
        ),
        /conflicting/
    )
    assert.equal(
        events.some((event) => event.type === 'SEAL'),
        false
    )
    assert.equal(events.at(-1)?.type, 'ABORT')
    assert.equal(f.calls.includes('HIST_SYNC'), false)
})

function reversedConversationPayload(count: number): Uint8Array {
    const record = proto.HistorySyncMsg.encode({
        message: {
            key: { id: 'synthetic', remoteJid: ADDRESS, fromMe: false },
            message: { conversation: 'synthetic' }
        }
    }).finish()
    const writer = proto.Conversation.encode({
        messages: Array.from({ length: count }, () => ({
            message: proto.HistorySyncMsg.decode(record).message
        }))
    })
    const bytes = concatBytes([
        writer.finish(),
        proto.Conversation.encode({ id: ADDRESS }).finish()
    ])
    // Obtain framing from the generated encoder, then replace its conversation payload.
    const framed = proto.HistorySync.encode({ syncType: 3 }).uint32(18).bytes(bytes).finish()
    return framed
}

for (const [name, payload, ceiling, pattern] of [
    [
        'record',
        () => historyPayload([{ conversation: 'x'.repeat(500) }]),
        { maxRecordBytes: 128 },
        /field|record|byte/i
    ],
    [
        'header',
        () => historyPayload([], { conversations: [{ id: ADDRESS, name: 'x'.repeat(500) }] }),
        { maxConversationHeaderBytes: 128 },
        /conversation-header/
    ],
    ['parked bytes', () => reversedConversationPayload(1), { maxParkedBytes: 1 }, /parked-byte/],
    [
        'private retained bytes',
        () => historyPayload([], { conversations: [{ id: ADDRESS, tcToken: new Uint8Array(32) }] }),
        { maxRetainedPrivateBytes: 1 },
        /private-retention/
    ]
] as const) {
    test(`resource ${name} overflow aborts without SEAL or source advancement`, async () => {
        const f = fixture()
        const events: WaHistorySyncStreamEventV2[] = []
        await assert.rejects(
            processHistorySyncNotification(
                f.deps,
                { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(payload()) },
                options(events, { ...RESOURCES, ...ceiling })
            ),
            pattern
        )
        assert.equal(
            events.some((event) => event.type === 'SEAL'),
            false
        )
        assert.equal(events.at(-1)?.type, 'ABORT')
        assert.equal(f.calls.includes('HIST_SYNC'), false)
        assert.equal(f.calls.includes('MESSAGE'), false)
    })
}

test('reversed field order is bounded without an extra private decoded-message parking collection', async () => {
    const f = fixture()
    const events: WaHistorySyncStreamEventV2[] = []
    await processHistorySyncNotification(
        f.deps,
        {
            syncType: 3,
            initialHistBootstrapInlinePayload: gzipSync(reversedConversationPayload(2))
        },
        { ...options(events), privateMailbox: 'PRESERVE' }
    )
    assert.equal(f.calls.filter((call) => call === 'MESSAGE').length, 2)
    assert.equal(events.at(-1)?.type, 'SEAL')
    await assert.rejects(
        processHistorySyncNotification(
            f.deps,
            {
                syncType: 3,
                initialHistBootstrapInlinePayload: gzipSync(reversedConversationPayload(3))
            },
            options([])
        ),
        /parked-message/
    )
})

test('disabled private mailbox avoids deferred body copies; preserve mode independently rejects their retention', async () => {
    const bytes = historyPayload([{ conversation: 'x'.repeat(1_024) }])
    const config = options([], { ...RESOURCES, maxRetainedPrivateBytes: 64 })
    const preserving = fixture()
    await assert.rejects(
        processHistorySyncNotification(
            preserving.deps,
            { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
            { ...config, privateMailbox: 'PRESERVE' }
        ),
        /private-retention/
    )
    assert.equal(preserving.calls.includes('MESSAGE'), false)
    assert.equal(preserving.calls.includes('HIST_SYNC'), false)
    const disabled = fixture()
    await processHistorySyncNotification(
        disabled.deps,
        { syncType: 3, initialHistBootstrapInlinePayload: gzipSync(bytes) },
        config
    )
    assert.equal(disabled.calls.includes('MESSAGE'), false)
    assert.equal(disabled.calls.at(-1), 'HIST_SYNC')
})

test('notification resource admission counts backing allocations before starting any capture', async () => {
    let processed = 0
    let failed = 0
    let settled = 0
    const worker = new HistorySyncBoundaryWorker(
        LIMITS,
        async () => {
            processed++
        },
        () => undefined,
        () => undefined,
        512
    )
    assert.equal(
        worker.enqueue(
            { syncType: 3, mediaKey: new Uint8Array(4_096).subarray(0, 32) },
            {
                complete: () => assert.fail('no receipt'),
                fail: () => {
                    failed++
                },
                sendHistorySyncReceipt: async () => assert.fail('no hist_sync')
            },
            () => {
                settled++
            }
        ),
        false
    )
    assert.equal(processed, 0)
    assert.equal(failed, 1)
    assert.equal(settled, 1)
    await worker.cancelAndDrain()
})

test('rich options require explicit positive resource ceilings; v2-only options reject v1 use', () => {
    const base = { sessionId: 'synthetic', store: { session: () => ({}) } }
    const valid = {
        onStream: async () => undefined,
        streamContent: 'RICH_V2',
        privateMailbox: 'DISABLED',
        boundaryLimits: LIMITS,
        streamLimits: STREAM_LIMITS
    }
    const resolve = (history: unknown) =>
        resolveWaClientBase({ ...base, history } as unknown as WaClientOptions, createNoopLogger())
    assert.doesNotThrow(() => resolve(valid))
    assert.throws(
        () => resolve({ ...valid, streamLimits: { ...STREAM_LIMITS, resources: undefined } }),
        /explicit stream resource/
    )
    assert.throws(() => resolve({ ...valid, streamContent: 'UNKNOWN' }), /unsupported/)
    assert.throws(() => resolve({ ...valid, privateMailbox: 'UNKNOWN' }), /unsupported/)
    assert.throws(() => resolve({ streamContent: 'RICH_V2' }), /require history.onStream/)
    assert.throws(() => resolve({ privateMailbox: 'DISABLED' }), /require history.onStream/)
    for (const key of Object.keys(RESOURCES)) {
        for (const value of [undefined, 0, -1, 1.5, Infinity, NaN]) {
            assert.throws(
                () =>
                    resolve({
                        ...valid,
                        streamLimits: {
                            ...STREAM_LIMITS,
                            resources: { ...RESOURCES, [key]: value }
                        }
                    }),
                /positive safe integer/
            )
        }
    }
})
