import type { WaHistorySyncSourceFactV2 } from '@client/types'
import { sha256 } from '@crypto/core'
import { proto } from '@proto'
import { bytesToHex, TEXT_ENCODER } from '@util/bytes'
import { PROTO_WIRE_TYPES, scanProtoFields } from '@util/protoscan'

type WithoutOrdinal<T> = T extends unknown ? Omit<T, 'ordinal'> : never
export type HistorySourceFactInput = WithoutOrdinal<WaHistorySyncSourceFactV2>

/** Only the already encoded root-15 mapping decision is projected; never names/avatars/other fields. */
export function projectHistoryIdentityAssertion(
    record: Uint8Array,
    sourceOccurrence: number,
    chargeField: () => void
): HistorySourceFactInput {
    let seen = 0
    let duplicate = false
    scanProtoFields(record, 0, record.length, (field) => {
        chargeField()
        if (
            (field.fieldNumber !== 1 && field.fieldNumber !== 2) ||
            field.wireType !== PROTO_WIRE_TYPES.LEN
        )
            throw new Error('unknown or malformed history identity mapping')
        const bit = 1 << field.fieldNumber
        if (seen & bit) duplicate = true
        seen |= bit
    })
    const source = 'HISTORY_SYNC_ROOT_15' as const
    const base = { source, evidenceVersion: 1 as const, sourceOccurrence }
    if (duplicate) return { ...base, kind: 'IDENTITY_UNRESOLVED', reason: 'DUPLICATE_COMPONENT' }
    const pair = proto.PhoneNumberToLIDMapping.decode(record)
    if (!pair.pnJid || !pair.lidJid)
        return { ...base, kind: 'IDENTITY_UNRESOLVED', reason: 'MISSING_PAIR' }
    // Strict user-address policy; do not strip devices, normalize digits or repair malformed identities.
    if (
        !/^[0-9]{1,64}@s\.whatsapp\.net$/.test(pair.pnJid) ||
        !/^[0-9]{1,64}@lid$/.test(pair.lidJid)
    )
        return { ...base, kind: 'IDENTITY_UNRESOLVED', reason: 'INVALID_PAIR' }
    const evidenceId = bytesToHex(
        sha256(TEXT_ENCODER.encode(JSON.stringify(['EXACT_V1', source, pair.pnJid, pair.lidJid])))
    )
    return {
        ...base,
        kind: 'IDENTITY_ASSERTION',
        evidenceId,
        phoneIdentity: pair.pnJid,
        lidIdentity: pair.lidJid,
        relation: 'EXPLICIT_PHONE_LID_PAIR',
        resolution: 'UNRESOLVED'
    }
}
