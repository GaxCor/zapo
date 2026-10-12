import type { WaResolvedMediaPayload } from '@message/encode/media-payload'

const sources = new WeakMap<WaHistorySyncMediaSourceV2, WaResolvedMediaPayload>()
const MEDIA_TYPES = new Set(['image', 'video', 'gif', 'audio', 'ptt', 'document', 'sticker'])

/**
 * @sensitive directPath, mediaKey, fileSha256, fileEncSha256.
 * Consumers are responsible for encryption at rest and must not JSON.stringify,
 * console.log or expose transfer inputs. Only explicitly export into protected
 * binary persistence/transfer paths. The handle rejects accidental JSON serialization
 * and masks Node inspection; it does not make exported inputs safe to log.
 */
export class WaHistorySyncMediaSourceV2 {
    /**
     * Copies validated descriptor bytes without retaining their source allocation.
     * @param payload Supported media locator and exact 32-byte key/hashes.
     * @throws If the locator, media crypto type or key/hash lengths are unsafe.
     */
    public constructor(payload: WaResolvedMediaPayload) {
        if (
            !MEDIA_TYPES.has(payload.mediaType) ||
            !isSafeHistoryMediaPath(payload.directPath) ||
            payload.mediaKey.byteLength !== 32 ||
            payload.fileSha256?.byteLength !== 32 ||
            payload.fileEncSha256?.byteLength !== 32
        ) {
            throw new Error('invalid protected history media descriptor')
        }
        // Detach small secrets from decoded record backing allocations and caller mutation.
        sources.set(this, {
            mediaType: payload.mediaType,
            directPath: payload.directPath,
            mediaKey: new Uint8Array(payload.mediaKey),
            fileSha256: new Uint8Array(payload.fileSha256),
            fileEncSha256: new Uint8Array(payload.fileEncSha256)
        })
        Object.freeze(this)
    }

    /**
     * Explicit sensitive export for protected persistence or the existing transfer API.
     * Returns detached copies; no retrieval is performed. The caller must impose
     * transfer limits and validate actual length/MIME separately using metadata.
     * @sensitive directPath, mediaKey, fileSha256, fileEncSha256 in the return value.
     */
    public getTransferInput(): WaResolvedMediaPayload {
        const source = sources.get(this)!
        return {
            mediaType: source.mediaType,
            directPath: source.directPath,
            mediaKey: source.mediaKey.slice(),
            fileSha256: source.fileSha256!.slice(),
            fileEncSha256: source.fileEncSha256!.slice()
        }
    }

    /** Conservative private handle charge; includes UTF-16 path, detached bytes and entry overhead. */
    public getRetainedByteLength(): number {
        return 512 + sources.get(this)!.directPath.length * 2 + 96
    }

    public toJSON(): never {
        throw new Error('protected history media requires explicit protected persistence')
    }

    public [Symbol.for('nodejs.util.inspect.custom')](): string {
        return 'WaHistorySyncMediaSourceV2 { PROTECTED }'
    }
}

/** Server-relative locator only; no arbitrary hosts, controls or encoded path traversal. */
function isSafeHistoryMediaPath(path: string): boolean {
    if (path.length > 4_096 || !/^\/(?!\/)[^\x00-\x20\\#]+$/.test(path)) return false
    try {
        return path
            .split('?')[0]
            .split('/')
            .slice(1)
            .every((part) => {
                const decoded = decodeURIComponent(part)
                return decoded !== '.' && decoded !== '..' && !/[\x00-\x20/\\%]/.test(decoded)
            })
    } catch {
        return false
    }
}
