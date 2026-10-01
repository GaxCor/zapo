import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { join as joinPath } from 'node:path'
import test from 'node:test'
import { setImmediate as yieldImmediate } from 'node:timers/promises'

import {
    type HistoryProtocolValidationContext,
    prepareBoundaryHistoryRecord,
    validateAcceptedHistoryRoot
} from '@client/persistence/history-protocol-dispositions'
import { proto } from '@proto'

function join(...parts: readonly Uint8Array[]): Uint8Array {
    const result = new Uint8Array(parts.reduce((n, p) => n + p.length, 0))
    let offset = 0
    for (const part of parts) {
        result.set(part, offset)
        offset += part.length
    }
    return result
}

function integer(value: bigint | number): Uint8Array {
    let left = BigInt(value)
    const bytes = []
    do {
        const next = left >> 7n
        bytes.push(Number(left & 127n) | (next ? 128 : 0))
        left = next
    } while (left)
    return Uint8Array.from(bytes)
}

function scalar(tag: number, value: bigint | number): Uint8Array {
    return join(integer(tag * 8), integer(value))
}

function len(tag: number, bytes: Uint8Array): Uint8Array {
    return join(integer(tag * 8 + 2), integer(bytes.length), bytes)
}

function str(tag: number, value: string): Uint8Array {
    return len(tag, new TextEncoder().encode(value))
}

function context(maxFieldBytes = 65_536, maxWork = 10_000, signal = new AbortController().signal) {
    let fields = 0
    return {
        value: {
            signal,
            maxFieldBytes,
            chargeField: () => {
                fields += 1
                if (fields > maxWork) throw new Error('work_limit')
            }
        } satisfies HistoryProtocolValidationContext,
        fields: () => fields
    }
}

async function root(tag: number, payload: Uint8Array, ctx = context().value) {
    const raw = len(tag, payload)
    await validateAcceptedHistoryRoot(tag, 2, raw, ctx)
}

function record(message: Uint8Array): Uint8Array {
    return len(
        1,
        join(
            len(
                1,
                join(
                    str(1, 'synthetic-peer@s.whatsapp.net'),
                    scalar(2, 0),
                    str(3, 'opaque-test-id')
                )
            ),
            len(2, message),
            scalar(3, 123)
        )
    )
}

function extended(bytes: Uint8Array): Uint8Array {
    return record(len(6, bytes))
}

test('R12 valid group, duplicate equal address, empty lists, both leave enums and uint64 edges', async () => {
    for (const reason of [0, 1]) {
        await root(
            12,
            join(
                str(1, 'synthetic-group@g.us'),
                str(1, 'synthetic-group@g.us'),
                len(
                    2,
                    join(
                        str(1, 'synthetic-member@s.whatsapp.net'),
                        scalar(2, reason),
                        scalar(3, 0xffff_ffff_ffff_ffffn)
                    )
                )
            )
        )
    }
    await root(12, str(1, 'synthetic-group@g.us'))
})

test('R12 rejects missing, empty, malformed, non-group and conflicting addresses without leaking them', async () => {
    for (const jid of [
        '',
        'synthetic-invalid',
        'synthetic@s.whatsapp.net',
        'synthetic@lid',
        'synthetic@broadcast',
        'synthetic@newsletter',
        'synthetic@other@g.us'
    ]) {
        await assert.rejects(root(12, str(1, jid)), /invalid_history_group_address/)
    }
    await assert.rejects(root(12, new Uint8Array()), /invalid_history_group_address/)
    await assert.rejects(
        root(12, join(str(1, 'synthetic-a@g.us'), str(1, 'synthetic-b@g.us'))),
        /conflicting_history_group_address/
    )
})

test('R12 rejects unknown children, invalid enum, wires, overflow and truncation', async () => {
    for (const nested of [
        scalar(99, 1),
        scalar(2, 2),
        len(2, new Uint8Array()),
        join(integer(3 * 8), new Uint8Array(11).fill(128)),
        new Uint8Array([10, 3, 1])
    ]) {
        await assert.rejects(root(12, join(str(1, 'synthetic@g.us'), len(2, nested))))
    }
    await assert.rejects(root(12, join(str(1, 'synthetic@g.us'), scalar(99, 1))))
})

test('R13 all pinned call enums, participants and signed int64 extremes; no call values returned', async () => {
    for (let call = 0; call <= 10; call += 1) {
        for (let silence = 0; silence <= 3; silence += 1) {
            const payload = proto.CallLogRecord.encode({
                callResult: call,
                silenceReason: silence,
                callType: call % 3,
                isDndMode: false,
                isVideo: true,
                isIncoming: false,
                isCallLink: true,
                callId: 'synthetic-private-call',
                callCreatorJid: 'synthetic-creator@s.whatsapp.net',
                groupJid: 'synthetic-group@g.us',
                scheduledCallId: 'synthetic-scheduled-call',
                callLinkToken: 'synthetic-token',
                participants: [
                    { userJid: 'synthetic@s.whatsapp.net', callResult: call },
                    { userJid: 'synthetic-second@s.whatsapp.net', callResult: call }
                ]
            }).finish()
            assert.equal(
                await root(
                    13,
                    join(
                        payload,
                        scalar(4, 0x8000_0000_0000_0000n),
                        scalar(5, 0x7fff_ffff_ffff_ffffn)
                    )
                ),
                undefined
            )
        }
    }
    await root(13, new Uint8Array())
})

test('R13 invalid enum/bool/wire/nested unknown and malformed string fail closed', async () => {
    for (const payload of [
        scalar(1, 11),
        scalar(3, 4),
        scalar(15, 3),
        scalar(2, 2),
        len(14, scalar(2, 11)),
        len(14, scalar(99, 1)),
        len(9, new Uint8Array([0xff])),
        new Uint8Array([74, 3, 1]),
        len(1, new Uint8Array())
    ]) {
        await assert.rejects(root(13, payload))
    }
})

const settings = {
    lightThemeWallpaper: { filename: 'synthetic-wallpaper', opacity: 0xffff_ffff, isGenAi: true },
    darkThemeWallpaper: { filename: '', opacity: 0 },
    mediaVisibility: 2,
    autoDownloadWiFi: {
        downloadImages: true,
        downloadAudio: false,
        downloadVideo: true,
        downloadDocuments: false
    },
    autoDownloadCellular: {},
    autoDownloadRoaming: {},
    showIndividualNotificationsPreview: true,
    showGroupNotificationsPreview: false,
    disappearingModeDuration: -1,
    disappearingModeTimestamp: 1,
    avatarUserSettings: { fbid: 'synthetic-fbid', password: 'synthetic-password' },
    fontSize: 1,
    securityNotifications: true,
    autoUnarchiveChats: false,
    videoQualityMode: -1,
    photoQualityMode: 2,
    individualNotificationSettings: {
        messageVibrate: '',
        messagePopup: '',
        messageLight: '',
        lowPriorityNotifications: true,
        reactionsMuted: false,
        callVibrate: ''
    },
    groupNotificationSettings: {},
    chatLockSettings: {
        hideLockedChats: true,
        secretCode: {
            encoding: 1,
            transformer: 2,
            transformerArg: [
                {
                    key: 'synthetic-key',
                    value: { asBlob: new Uint8Array([255]), asUnsignedInteger: 0xffff_ffff }
                }
            ],
            transformedData: new Uint8Array([255])
        }
    },
    chatDbLidMigrationTimestamp: 2
}

test('R08 every pinned settings field, empty children, duplicated roots/children, no decoded return', async () => {
    await root(8, new Uint8Array())
    const all = proto.GlobalSettings.encode(settings).finish()
    await root(8, join(all, all))
    for (const [key, value] of Object.entries(settings))
        await root(8, proto.GlobalSettings.encode({ [key]: value }).finish())
    for (const mediaVisibility of [0, 1, 2])
        await root(8, proto.GlobalSettings.encode({ mediaVisibility }).finish())
    for (const encoding of [0, 1])
        for (const transformer of [0, 1, 2])
            await root(
                8,
                proto.GlobalSettings.encode({
                    chatLockSettings: { secretCode: { encoding, transformer } }
                }).finish()
            )
})

test('R08 unknown at every nested type and earlier bad duplicate cannot be overwritten', async () => {
    const paths = [
        [],
        [1],
        [3],
        [4],
        [5],
        [6],
        [11],
        [17],
        [18],
        [19],
        [19, 2],
        [19, 2, 3],
        [19, 2, 3, 2]
    ]
    for (const path of paths) {
        let bad = scalar(999, 1)
        for (const tag of [...path].reverse()) bad = len(tag, bad)
        await assert.rejects(root(8, join(bad, proto.GlobalSettings.encode(settings).finish())))
    }
    for (const bad of [
        scalar(2, 3),
        scalar(7, 2),
        len(11, len(2, new Uint8Array([255]))),
        len(19, len(2, scalar(1, 2))),
        len(19, len(2, scalar(2, 3))),
        scalar(9, 0x8000_0000n),
        scalar(12, 0xffff_ffffn)
    ])
        await assert.rejects(root(8, bad))
})

test('R09 accepts empty/arbitrary bytes with no UTF8 or nested semantics and exact ceiling', async () => {
    for (const bytes of [new Uint8Array(), new Uint8Array([255, 128, 0]), new Uint8Array(16)])
        await root(9, bytes, context(16).value)
    await assert.rejects(root(9, new Uint8Array(17), context(16).value), /oversized/)
})

test('R10 strict uint32, nonminimal encoding and every malformed varint', async () => {
    for (const value of [0, 1, 0xffff_ffff])
        await validateAcceptedHistoryRoot(10, 0, scalar(10, value), context().value)
    await validateAcceptedHistoryRoot(
        10,
        0,
        join(integer(80), new Uint8Array([128, 0])),
        context().value
    )
    for (const bytes of [
        scalar(10, 0x1_0000_0000n),
        scalar(10, 0xffff_ffff_ffff_ffffn),
        join(integer(80), new Uint8Array([128])),
        join(integer(80), new Uint8Array(11).fill(128)),
        join(integer(80), new Uint8Array(9).fill(128), new Uint8Array([2]))
    ])
        await assert.rejects(validateAcceptedHistoryRoot(10, 0, bytes, context().value))
})

for (const tag of [8, 9, 10, 12, 13]) {
    test(`root ${tag}: all wrong wires, illegal groups, malformed framing and unrelated root rejected`, async () => {
        for (const wire of [0, 1, 2, 3, 4, 5, 6, 7]) {
            if (wire === (tag === 10 ? 0 : 2)) continue
            const payload =
                wire === 0
                    ? new Uint8Array([0])
                    : wire === 1
                      ? new Uint8Array(8)
                      : wire === 2
                        ? new Uint8Array([0])
                        : wire === 5
                          ? new Uint8Array(4)
                          : new Uint8Array()
            await assert.rejects(
                validateAcceptedHistoryRoot(
                    tag,
                    wire,
                    join(integer(tag * 8 + wire), payload),
                    context().value
                )
            )
        }
        await assert.rejects(
            validateAcceptedHistoryRoot(
                tag,
                2,
                new Uint8Array([tag * 8 + 2, 8, 1]),
                context().value
            )
        )
        await assert.rejects(
            validateAcceptedHistoryRoot(tag, 2, len(11, new Uint8Array()), context().value)
        )
    })
}

test('E04 outer, quoted and recursively quoted occurrences; exact text/context preserved', async () => {
    const quoted = len(6, join(str(1, 'quoted-synthetic-body'), str(4, 'synthetic-url-quoted')))
    const outer = join(
        str(1, ''),
        str(4, 'synthetic-url-outer'),
        str(4, ''),
        len(17, join(str(4, 'synthetic-context-address'), len(3, quoted)))
    )
    const input = extended(outer)
    const ctx = context()
    const clean = await prepareBoundaryHistoryRecord(input, ctx.value)
    assert.equal(clean.outerCount, 2)
    assert.equal(clean.quotedCount, 1)
    assert.ok(clean.bytes.length < input.length)
    const decoded = proto.HistorySyncMsg.decode(clean.bytes).message!.message!.extendedTextMessage!
    assert.equal(decoded.text, '')
    assert.equal(decoded.contextInfo!.remoteJid, 'synthetic-context-address')
    assert.equal(
        decoded.contextInfo!.quotedMessage!.extendedTextMessage!.text,
        'quoted-synthetic-body'
    )
    assert.equal(decoded.$unknowns?.length ?? 0, 0)
    assert.equal(new TextDecoder().decode(clean.bytes).includes('synthetic-url'), false)
    assert.equal(
        new TextDecoder().decode(new Uint8Array(clean.bytes.buffer)).includes('synthetic-url'),
        false
    )
    assert.ok(ctx.fields() > 0)
    const repeated = await prepareBoundaryHistoryRecord(
        extended(join(outer, outer)),
        context().value
    )
    assert.equal(repeated.outerCount, 4)
    assert.equal(repeated.quotedCount, 2)
})

test('E04 known other content/ContextInfo quote reaches exact type without granting canonical text', async () => {
    const image = len(3, len(17, len(3, len(6, str(4, 'synthetic-hidden-url')))))
    const clean = await prepareBoundaryHistoryRecord(record(image), context().value)
    assert.equal(clean.quotedCount, 1)
    assert.equal(clean.outerCount, 0)
    assert.ok(proto.HistorySyncMsg.decode(clean.bytes).message!.message!.imageMessage)
})

test('E04 absent and strict UTF8 boundary strings, no canonical URL semantic validation', async () => {
    const unchanged = extended(str(1, 'synthetic-text'))
    assert.equal((await prepareBoundaryHistoryRecord(unchanged, context().value)).bytes, unchanged)
    for (const value of [
        '',
        '\u0000',
        '\u007f',
        '\u0080',
        '\u0800',
        '\ud7ff',
        '\ue000',
        '\u{10000}',
        '\u{10ffff}',
        'not a URL'
    ]) {
        const prepared = await prepareBoundaryHistoryRecord(
            extended(join(str(1, 'synthetic-text'), str(4, value))),
            context().value
        )
        assert.equal(prepared.outerCount, 1)
        assert.equal(
            proto.HistorySyncMsg.decode(prepared.bytes).message!.message!.extendedTextMessage!.text,
            'synthetic-text'
        )
    }
})

test('E04 projection preserves unrelated nonminimal child framing and duplicate .text bytes', async () => {
    const contextPayload = str(4, 'synthetic-context')
    const contextField = join(
        integer(17 * 8 + 2),
        new Uint8Array([contextPayload.length | 128, 0]),
        contextPayload
    )
    const expectedExtended = join(str(1, 'first synthetic text'), str(1, ''), contextField)
    const clean = await prepareBoundaryHistoryRecord(
        extended(join(expectedExtended, str(4, 'synthetic-url'))),
        context().value
    )
    assert.deepEqual(clean.bytes, extended(expectedExtended))
})

test('E04 rejects bad UTF8, every wrong wire, unknown alongside URL and overwritten bad child', async () => {
    for (const invalid of [
        [0xff],
        [0xc0, 0x80],
        [0xed, 0xa0, 0x80],
        [0xf4, 0x90, 0x80, 0x80],
        [0xe0, 0x80, 0x80],
        [0xc2],
        [0xf0, 0x90, 0x80]
    ])
        await assert.rejects(
            prepareBoundaryHistoryRecord(
                extended(len(4, Uint8Array.from(invalid))),
                context().value
            ),
            /utf8/
        )
    for (const bad of [
        scalar(4, 0),
        join(integer(4 * 8 + 1), new Uint8Array(8)),
        new Uint8Array([34, 5, 1]),
        join(str(4, 'synthetic-url'), scalar(99, 1)),
        join(len(17, scalar(999, 1)), len(17, new Uint8Array()))
    ])
        await assert.rejects(prepareBoundaryHistoryRecord(extended(bad), context().value))
    await assert.rejects(
        prepareBoundaryHistoryRecord(
            record(join(len(6, scalar(99, 1)), len(6, str(4, 'synthetic-url')))),
            context().value
        )
    )
    // ContextInfo field 4 is a KNOWN string, not an exclusion; wrong wire still rejects.
    await assert.rejects(
        prepareBoundaryHistoryRecord(extended(len(17, scalar(4, 1))), context().value)
    )
    await assert.rejects(
        prepareBoundaryHistoryRecord(record(len(4, str(99, 'synthetic-url'))), context().value)
    )
    for (const wire of [0, 1, 3, 4, 5, 6, 7]) {
        const payload =
            wire === 0
                ? new Uint8Array([0])
                : wire === 1
                  ? new Uint8Array(8)
                  : wire === 5
                    ? new Uint8Array(4)
                    : new Uint8Array()
        await assert.rejects(
            prepareBoundaryHistoryRecord(
                extended(join(integer(4 * 8 + wire), payload)),
                context().value
            )
        )
    }
    // ConsumerApplication is a different, unreachable protocol type; it has no rule here.
    // Its actual pinned tag 4 is description, not Message.ExtendedTextMessage's URL.
    const consumer = proto.ConsumerApplication.ExtendedTextMessage.encode({
        description: 'synthetic-description'
    }).finish()
    assert.deepEqual(Uint8Array.from(consumer), str(4, 'synthetic-description'))
    await assert.rejects(
        prepareBoundaryHistoryRecord(consumer, context().value),
        /unknown_or_missing_message_record/
    )
})

test('BND exact record/field/work ceilings, zero-size repeated child accounting and no second-pass charge', async () => {
    const input = extended(join(str(1, 'synthetic'), str(4, 'synthetic-url')))
    const measured = context(input.length)
    const expected = await prepareBoundaryHistoryRecord(input, measured.value)
    assert.equal(
        (await prepareBoundaryHistoryRecord(input, context(input.length, measured.fields()).value))
            .outerCount,
        expected.outerCount
    )
    await assert.rejects(
        prepareBoundaryHistoryRecord(input, context(input.length, measured.fields() - 1).value),
        /work_limit/
    )
    await assert.rejects(
        prepareBoundaryHistoryRecord(input, context(input.length - 1).value),
        /record_limit/
    )
    await root(
        13,
        join(len(14, new Uint8Array()), len(14, new Uint8Array())),
        context(100, 2).value
    )
    await assert.rejects(
        root(13, join(len(14, new Uint8Array()), len(14, new Uint8Array())), context(100, 1).value),
        /work_limit/
    )
})

test('BND fixed32/fixed64 widths and illegal nested groups reject', async () => {
    await prepareBoundaryHistoryRecord(
        extended(join(integer(7 * 8 + 5), new Uint8Array(4))),
        context().value
    )
    const withDouble = proto.HistorySyncMsg.encode({
        message: { message: { locationMessage: { degreesLatitude: 1.5 } } }
    }).finish()
    await prepareBoundaryHistoryRecord(withDouble, context().value)
    for (const field of [
        join(integer(7 * 8 + 5), new Uint8Array(3)),
        integer(4 * 8 + 3),
        integer(4 * 8 + 4),
        integer(4 * 8 + 7)
    ])
        await assert.rejects(prepareBoundaryHistoryRecord(extended(field), context().value))
})

test('BND finite depth permits 32 and rejects 33 including recursive quote containers', async () => {
    // HistorySyncMsg -> WebMessageInfo -> Message is depth 2; each FutureProofMessage adds 2.
    let at32 = len(1, new Uint8Array())
    for (let i = 0; i < 15; i += 1) at32 = len(37, len(1, at32))
    await prepareBoundaryHistoryRecord(record(at32), context().value)
    await assert.rejects(
        prepareBoundaryHistoryRecord(record(len(37, len(1, at32))), context().value),
        /depth_limit/
    )
    let quote = len(6, str(4, 'synthetic-url'))
    for (let i = 0; i < 12; i += 1) quote = len(6, len(17, len(3, quote)))
    await assert.rejects(
        prepareBoundaryHistoryRecord(record(quote), context().value),
        /depth_limit/
    )
})

test('BND long secret/UTF8 scans yield to independent live work and cooperatively abort', async () => {
    for (const text of [false, true]) {
        const controller = new AbortController()
        let live = false
        const data = new Uint8Array(100_000).fill(97)
        const pending = text
            ? prepareBoundaryHistoryRecord(
                  extended(len(4, data)),
                  context(200_000, 100, controller.signal).value
              )
            : root(9, data, context(200_000, 100, controller.signal).value)
        void yieldImmediate().then(() => {
            live = true
            controller.abort(new Error('synthetic-sensitive-error'))
        })
        await assert.rejects(pending, /history_protocol_cancelled/)
        assert.equal(live, true)
    }
})

test('closed pinned rules cover reachable consumed message/root types and exact exclusion is unique', () => {
    // Test-only source check, not reflection or descriptor loading in production.
    const helper = readFileSync(
        joinPath(process.cwd(), 'src/client/persistence/history-protocol-dispositions.ts'),
        'utf8'
    )
    const source = readFileSync(joinPath(process.cwd(), 'spec/proto/WAProto.proto'), 'utf8')
    const tableText = helper.slice(
        helper.indexOf(' = {') + 3,
        helper.indexOf('\n}\n', helper.indexOf('const PINNED_RULES')) + 2
    )
    const table = JSON.parse(
        tableText
            .replace(/'/g, '"')
            .replace(/([{,]\s*)([\w$]+)\s*:/g, '$1"$2":')
            .replace(/,\s*([}\]])/g, '$1')
    ) as Record<string, Record<string, unknown>>
    const stack: string[] = []
    const fields = new Map<string, Set<string>>()
    for (const line of source.split(/\r?\n/)) {
        const declaration = line.match(/^\s*(message|enum) (\w+) \{/)
        if (declaration) {
            stack.push(declaration[2])
            fields.set(stack.join('.'), new Set())
        } else if (/^\s*\}/.test(line)) stack.pop()
        else {
            const field =
                line.match(/^\s*(?:optional|required|repeated) [\w.]+ \w+ = (\d+)/) ??
                line.match(/^\s*map<[^>]+> \w+ = (\d+)/)
            if (field) fields.get(stack.join('.'))!.add(field[1])
        }
    }
    let exclusions = 0
    assert.equal(
        Object.keys(table).some((type) => type.startsWith('ConsumerApplication')),
        false
    )
    for (const [type, row] of Object.entries(table)) {
        if (type.endsWith('Entry')) continue // The two pinned map-entry wire records are implicit schema types.
        const expected = fields.get(type)
        assert.ok(expected, type)
        for (const [tag, rule] of Object.entries(row)) {
            if (rule === 'excludedCanonicalUrl') {
                assert.equal(type, 'Message.ExtendedTextMessage')
                assert.equal(tag, '4')
                exclusions += 1
            } else assert.equal(expected.has(tag), true, type + '/' + tag)
        }
        assert.equal(
            Object.keys(row).length,
            expected.size + (type === 'Message.ExtendedTextMessage' ? 1 : 0),
            type
        )
    }
    assert.equal(exclusions, 1)
})

test('packed and unpacked pinned uint32/enum forms validate every element without wrapping', async () => {
    const metadata = proto.MessageContextInfo.encode({
        deviceListMetadata: { senderKeyIndexes: [0, 0xffff_ffff] }
    }).finish()
    await prepareBoundaryHistoryRecord(record(len(35, metadata)), context().value)
    // Message.messageContextInfo=35 -> deviceListMetadata=1 -> senderKeyIndexes=3.
    await prepareBoundaryHistoryRecord(record(len(35, len(1, scalar(3, 1)))), context().value)
    await assert.rejects(
        prepareBoundaryHistoryRecord(
            record(len(35, len(1, len(3, integer(0x1_0000_0000n))))),
            context().value
        ),
        /uint32/
    )
})
