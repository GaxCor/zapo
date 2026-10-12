import { WaHistorySyncMediaSourceV2 } from '@client/persistence/WaHistorySyncMediaSourceV2'
import type {
    WaHistorySyncContentV2,
    WaHistorySyncMediaMetadataV2,
    WaHistorySyncMessageObservationV2
} from '@client/types'
import { getContentType } from '@message/encode/content'
import { resolveMediaPayload } from '@message/encode/media-payload'
import type { Proto } from '@proto'
import { bytesToBase64 } from '@util/bytes'

const MIME_TYPES: Readonly<Record<string, readonly string[]>> = {
    IMAGE: ['image/jpeg', 'image/png', 'image/webp', 'image/gif'],
    VIDEO: ['video/mp4', 'video/3gpp', 'video/webm'],
    AUDIO: [
        'audio/ogg',
        'audio/mpeg',
        'audio/mp4',
        'audio/aac',
        'audio/wav',
        'audio/webm',
        'audio/amr'
    ],
    VOICE_NOTE: ['audio/ogg', 'audio/mpeg', 'audio/mp4', 'audio/aac', 'audio/amr'],
    DOCUMENT: [
        'application/pdf',
        'text/plain',
        'application/zip',
        'application/octet-stream',
        'application/msword',
        'application/vnd.ms-excel',
        'application/vnd.ms-powerpoint',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation'
    ],
    STICKER: ['image/webp']
}

interface MediaFields {
    readonly mimetype?: string | null
    readonly fileLength?: number | { toString(): string } | null
    readonly caption?: string | null
    readonly fileName?: string | null
    readonly width?: number | null
    readonly height?: number | null
    readonly seconds?: number | null
    readonly ptt?: boolean | null
    readonly gifPlayback?: boolean | null
    readonly isAnimated?: boolean | null
    readonly viewOnce?: boolean | null
    readonly jpegThumbnail?: Uint8Array | null
    readonly contextInfo?: Proto.IContextInfo | null
}

/** Projection of already validated supported source fields; no unwrap, requests or receipt authority. */
export function projectHistoryContentV2(
    message: Proto.IMessage,
    maxThumbnailBytes: number
): Pick<WaHistorySyncMessageObservationV2, 'content' | 'context' | 'mediaSource'> {
    const type = getSingleContentType(message)
    if (!type) return { content: { kind: 'UNSUPPORTED' } }
    const unsupported = { content: { kind: 'UNSUPPORTED' as const, kindHint: type } }
    if (type === 'conversation') return { content: { kind: 'TEXT', text: message.conversation! } }
    if (type === 'extendedTextMessage') {
        const fields = message.extendedTextMessage!
        return {
            content:
                typeof fields.text === 'string'
                    ? { kind: 'TEXT', text: fields.text }
                    : unsupported.content,
            ...projectContext(fields.contextInfo)
        }
    }
    if (type === 'locationMessage') {
        const fields = message.locationMessage!
        if (
            !Number.isFinite(fields.degreesLatitude) ||
            !Number.isFinite(fields.degreesLongitude) ||
            fields.degreesLatitude! < -90 ||
            fields.degreesLatitude! > 90 ||
            fields.degreesLongitude! < -180 ||
            fields.degreesLongitude! > 180
        )
            return unsupported
        return {
            content: {
                kind: 'LOCATION',
                latitude: fields.degreesLatitude!,
                longitude: fields.degreesLongitude!,
                ...(typeof fields.name === 'string' ? { name: fields.name } : {}),
                ...(typeof fields.address === 'string' ? { address: fields.address } : {})
            },
            ...projectContext(fields.contextInfo)
        }
    }
    if (type === 'contactMessage' || type === 'contactsArrayMessage') {
        const fields =
            type === 'contactMessage' ? message.contactMessage! : message.contactsArrayMessage!
        const cards =
            type === 'contactMessage'
                ? [message.contactMessage!]
                : (message.contactsArrayMessage!.contacts ?? [])
        return {
            content: {
                kind: 'CONTACT',
                contacts: cards.map((card) => ({
                    ...(typeof card.displayName === 'string'
                        ? { displayName: card.displayName }
                        : {}),
                    ...(typeof card.vcard === 'string' ? { vcard: card.vcard } : {})
                }))
            },
            ...projectContext(fields.contextInfo)
        }
    }
    let kind: Extract<WaHistorySyncContentV2, { media: unknown }>['kind']
    let fields: MediaFields
    switch (type) {
        case 'imageMessage':
            kind = 'IMAGE'
            fields = message.imageMessage!
            break
        case 'videoMessage':
            kind = 'VIDEO'
            fields = message.videoMessage!
            break
        case 'audioMessage':
            kind = message.audioMessage!.ptt === true ? 'VOICE_NOTE' : 'AUDIO'
            fields = message.audioMessage!
            break
        case 'documentMessage':
            kind = 'DOCUMENT'
            fields = message.documentMessage!
            break
        case 'stickerMessage':
            kind = 'STICKER'
            fields = message.stickerMessage!
            break
        default:
            return unsupported // Wrappers (including view-once/ephemeral), events and other kinds stay closed.
    }
    if (fields.viewOnce === true) return { content: { kind: 'UNSUPPORTED', kindHint: 'viewOnce' } }
    const mime = typeof fields.mimetype === 'string' ? fields.mimetype : undefined
    const safeMime =
        mime !== undefined &&
        mime.length <= 255 &&
        !/[\x00-\x1f\x7f]/.test(mime) &&
        MIME_TYPES[kind].includes(mime.split(';')[0].trim().toLowerCase())
    let mediaSource: WaHistorySyncMediaSourceV2 | undefined
    let retrievalReason: WaHistorySyncMediaMetadataV2['retrievalReason'] = 'UNSAFE_MIME'
    if (safeMime) {
        // The existing transfer helper converts length to a JS number. Metadata
        // keeps its exact uint64 separately; only descriptor fields enter the handle.
        const payload = resolveMediaPayload({ [type]: { ...fields, fileLength: undefined } })
        retrievalReason = 'MISSING_DESCRIPTOR'
        if (payload) {
            try {
                mediaSource = new WaHistorySyncMediaSourceV2(payload)
            } catch {
                retrievalReason = 'INVALID_DESCRIPTOR'
            }
        }
    }
    const thumbnail = fields.jpegThumbnail
    const thumbnailState = !thumbnail?.byteLength
        ? 'ABSENT'
        : thumbnail.byteLength > maxThumbnailBytes
          ? 'OMITTED_LIMIT'
          : thumbnail[0] === 0xff && thumbnail[1] === 0xd8 && thumbnail[2] === 0xff
            ? 'AVAILABLE'
            : 'UNSUPPORTED_FORMAT'
    const fileLength = decimalUint64(fields.fileLength)
    const media: WaHistorySyncMediaMetadataV2 = {
        ...(safeMime ? { mimetype: mime.split(';')[0].trim().toLowerCase() } : {}),
        ...(fileLength !== undefined ? { fileLength } : {}),
        ...(typeof fields.caption === 'string' ? { caption: fields.caption } : {}),
        ...(typeof fields.fileName === 'string'
            ? { fileName: sanitizeFileName(fields.fileName) }
            : {}),
        ...numberFields(fields),
        ...(typeof fields.ptt === 'boolean' ? { ptt: fields.ptt } : {}),
        ...(typeof fields.gifPlayback === 'boolean' ? { gifPlayback: fields.gifPlayback } : {}),
        ...(typeof fields.isAnimated === 'boolean' ? { isAnimated: fields.isAnimated } : {}),
        thumbnailState,
        ...(thumbnailState === 'AVAILABLE'
            ? { thumbnail: { mimetype: 'image/jpeg', base64: bytesToBase64(thumbnail!) } as const }
            : {}),
        retrievalState: mediaSource ? 'DESCRIPTOR_AVAILABLE' : 'UNAVAILABLE',
        ...(!mediaSource ? { retrievalReason } : {})
    }
    return {
        content: { kind, media },
        ...projectContext(fields.contextInfo),
        ...(mediaSource ? { mediaSource } : {})
    }
}

function numberFields(fields: MediaFields): { width?: number; height?: number; seconds?: number } {
    const output: { width?: number; height?: number; seconds?: number } = {}
    for (const key of ['width', 'height', 'seconds'] as const) {
        const value = fields[key]
        if (Number.isSafeInteger(value) && value! >= 0) output[key] = value!
    }
    return output
}

function projectContext(
    info: Proto.IContextInfo | null | undefined
): Pick<WaHistorySyncMessageObservationV2, 'context'> {
    if (!info) return {}
    const context: NonNullable<WaHistorySyncMessageObservationV2['context']> = {
        ...(typeof info.isForwarded === 'boolean' ? { isForwarded: info.isForwarded } : {}),
        ...(Number.isSafeInteger(info.forwardingScore) && info.forwardingScore! >= 0
            ? { forwardingScore: info.forwardingScore! }
            : {}),
        ...(typeof info.stanzaId === 'string' && info.stanzaId.length
            ? {
                  quote: {
                      externalMessageId: info.stanzaId,
                      ...(typeof info.participant === 'string'
                          ? { participant: info.participant }
                          : {}),
                      ...(typeof info.remoteJid === 'string' ? { remoteJid: info.remoteJid } : {}),
                      ...(info.quotedMessage ? { preview: quotePreview(info.quotedMessage) } : {})
                  }
              }
            : {})
    }
    return Object.keys(context).length ? { context } : {}
}

function quotePreview(
    message: Proto.IMessage
): NonNullable<NonNullable<WaHistorySyncMessageObservationV2['context']>['quote']>['preview'] {
    const type = getSingleContentType(message)
    if (type === 'conversation' && typeof message.conversation === 'string')
        return { kind: 'TEXT', text: message.conversation }
    if (type === 'extendedTextMessage' && typeof message.extendedTextMessage?.text === 'string')
        return { kind: 'TEXT', text: message.extendedTextMessage.text }
    return { kind: 'UNSUPPORTED', ...(type ? { kindHint: type } : {}) }
}

function getSingleContentType(message: Proto.IMessage): keyof Proto.IMessage | undefined {
    const type = getContentType(message)
    if (!type) return undefined
    // Use the accepted selector on the remainder so a second payload cannot
    // silently inherit the first payload's content or quote authority.
    const remaining = { ...message }
    delete remaining[type]
    if (getContentType(remaining)) throw new Error('conflicting rich history content')
    return type
}

function decimalUint64(value: MediaFields['fileLength']): string | undefined {
    if (
        value === null ||
        value === undefined ||
        (typeof value === 'number' && !Number.isSafeInteger(value))
    )
        return undefined
    const text = value.toString()
    if (!/^(0|[1-9]\d*)$/.test(text) || BigInt(text) > 0xffff_ffff_ffff_ffffn) return undefined
    return text
}

function sanitizeFileName(value: string): string {
    const name = value
        .split(/[\\/]/)
        .at(-1)!
        .replace(/[\x00-\x1f\x7f\u202a-\u202e\u2066-\u2069<>:"|?*]/g, '_')
        .replace(/[. ]+$/g, '')
    const bounded = Array.from(name).slice(0, 200).join('') || 'attachment'
    return /^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i.test(bounded) ? `_${bounded}` : bounded
}
