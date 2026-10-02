import { flushPendingWrites, openHistoryBlobStream } from '@client/persistence/history-blob'
import {
    historyFailure,
    type HistoryFailureReason,
    markHistoryFailure,
    waitHistoryCleanup
} from '@client/persistence/history-failure'
import {
    type HistoryProtocolValidationContext,
    prepareBoundaryHistoryRecord,
    validateAcceptedHistoryRoot
} from '@client/persistence/history-protocol-dispositions'
import type { WriteBehindPersistence } from '@client/persistence/WriteBehindPersistence'
import type {
    WaClientEventMap,
    WaHistorySyncBatchHandler,
    WaHistorySyncBoundaryLimits,
    WaHistorySyncChatKind,
    WaHistorySyncChunkEvent,
    WaHistorySyncFamilyDisposition,
    WaHistorySyncMessageObservation
} from '@client/types'
import type { Logger } from '@infra/log/types'
import type { WaMediaTransferClient } from '@media/transfer/WaMediaTransferClient'
import { getContentType } from '@message/encode/content'
import { resolveWebMessageInfoAuthor } from '@message/primitives/incoming'
import { proto, type Proto } from '@proto'
import {
    isBroadcastJid,
    isGroupJid,
    isHostedServer,
    isLidJid,
    isNewsletterJid,
    isUserJid,
    splitJid
} from '@protocol/jid'
import { normalizeEphemeralSettingSeconds } from '@protocol/message'
import { normalizeUsername } from '@protocol/username'
import type { WaChatMetadataStore } from '@store/contracts/chat-metadata.store'
import { concatBytes, TEXT_DECODER } from '@util/bytes'
import { longToNumber, toError } from '@util/primitives'
import {
    PROTO_STREAM_EVENT_KINDS,
    type ProtoStreamEvent,
    streamProtoFields
} from '@util/proto-stream'
import { PROTO_WIRE_TYPES } from '@util/protoscan'

const HANDLED_SYNC_TYPES = new Set([
    proto.Message.HistorySyncType.INITIAL_BOOTSTRAP,
    proto.Message.HistorySyncType.RECENT,
    proto.Message.HistorySyncType.FULL,
    proto.Message.HistorySyncType.PUSH_NAME,
    proto.Message.HistorySyncType.ON_DEMAND,
    proto.Message.HistorySyncType.NON_BLOCKING_DATA
])
const HISTORY_SYNC_MAX_PENDING_WRITES = 1_024
const HISTORY_SYNC_MAX_RECORD_BYTES = 16 * 1024 * 1024
/**
 * Ceiling on messages held while waiting for their `Conversation.id`. Field order
 * puts the id first, so this only fills if a serializer reverses it, and letting
 * it grow would rebuild the very peak the streaming pass removes. Exceeding it
 * fails the chunk rather than dropping records: an aborted chunk stays unacked
 * and gets resent, while a dropped one would be acked and lost for good.
 */
const HISTORY_SYNC_MAX_PARKED_MESSAGES = 1_024

export const HISTORY_SYNC_FIELDS = Object.freeze({
    SYNC_TYPE: 1,
    CONVERSATIONS: 2,
    CHUNK_ORDER: 5,
    PROGRESS: 6,
    PUSHNAMES: 7,
    PHONE_NUMBER_TO_LID_MAPPINGS: 15,
    NCT_SALT: 19,
    INLINE_CONTACTS: 20
} as const)

export const CONVERSATION_FIELDS = Object.freeze({
    ID: 1,
    MESSAGES: 2
} as const)

interface WaHistorySyncPrivacyTokenConversation {
    readonly jid: string
    readonly tcToken?: Uint8Array | null
    readonly tcTokenTimestamp?: number | null
    readonly tcTokenSenderTimestamp?: number | null
}

interface WaHistorySyncDeps {
    readonly logger: Logger
    readonly mediaTransfer: WaMediaTransferClient
    readonly writeBehind: WriteBehindPersistence
    readonly chatMetadataStore?: WaChatMetadataStore
    readonly emitEvent: <K extends keyof WaClientEventMap>(
        event: K,
        ...args: Parameters<WaClientEventMap[K]>
    ) => void
    readonly onPrivacyTokens?: (
        conversations: readonly WaHistorySyncPrivacyTokenConversation[]
    ) => Promise<void>
    readonly onNctSalt?: (salt: Uint8Array) => Promise<void>
    /** Acks the chunk via the `hist_sync` receipt so the primary stops resending it. */
    readonly onProcessed?: (syncType: Proto.Message.HistorySyncType) => Promise<void>
    /** Author fallback for self-sent group messages that carry no participant. */
    readonly meJid?: string | null
}

interface ParkedHistoryMessage {
    readonly id: string
    /**
     * The author-bearing fields of the record, trimmed off the decoded
     * `WebMessageInfo` so parking a message never retains the whole envelope.
     */
    readonly authorInfo: Proto.IWebMessageInfo
    readonly fromMe: boolean
    readonly timestampMs: number | undefined
    readonly messageBytes: Uint8Array
}

interface HistoryBoundaryMessageContext {
    readonly limits: WaHistorySyncBoundaryLimits
    readonly signal: AbortSignal
    readonly onBatch: WaHistorySyncBatchHandler
}

interface HistoryBoundaryCapture {
    readonly context: HistoryBoundaryMessageContext
    readonly notification: Proto.Message.IHistorySyncNotification
    readonly observations: WaHistorySyncMessageObservation[]
    readonly familyDispositions: Map<string, WaHistorySyncFamilyDisposition>
    readonly metadataConflicts: Set<
        'notification_payload_chunk_order_mismatch' | 'notification_payload_progress_mismatch'
    >
    readonly deferredMessageWrites: Array<() => Promise<void>>
    blockedReason: Error | null
    decodedSyncType: number | null
    sourceRecordCount: number
}

interface HistorySyncChunkState {
    readonly writeBehind: WriteBehindPersistence
    readonly maxOutstandingHistoryWrites: number
    readonly nowMs: number
    readonly pendingWrites: Promise<void>[]
    readonly pushnames: Proto.IPushname[]
    readonly inlineContacts: Proto.IInlineContact[]
    readonly pnToLid: Map<string, string>
    readonly tokenConversations: WaHistorySyncPrivacyTokenConversation[]
    readonly boundary?: HistoryBoundaryCapture
    readonly boundaryParkedRecords: Uint8Array[]
    nctSalt: Uint8Array | null
    chunkOrder: number | null
    progress: number | null
    conversationsCount: number
    messagesCount: number
}

export async function runHistorySyncNotification(
    deps: WaHistorySyncDeps,
    notification: Proto.Message.IHistorySyncNotification
): Promise<void> {
    try {
        await processHistorySyncNotification(deps, notification)
    } catch (error) {
        deps.logger.warn('failed to process history sync notification', {
            syncType: notification.syncType,
            chunkOrder: notification.chunkOrder,
            message: toError(error).message
        })
    }
}

/**
 * Applies one history sync chunk while it downloads, descending into
 * `conversations` so peak memory tracks the largest single message and not the
 * chunk. Chunk-wide records settle after the stream, once every LID mapping is in.
 */
export async function processHistorySyncNotification(
    deps: WaHistorySyncDeps,
    notification: Proto.Message.IHistorySyncNotification,
    boundaryContext?: HistoryBoundaryMessageContext
): Promise<void> {
    if (boundaryContext && notification.$unknowns?.length) {
        throw new Error('history boundary notification contains unknown fields')
    }
    const syncType = notification.syncType
    if (syncType === null || syncType === undefined) {
        if (boundaryContext) {
            throw new Error('history boundary notification is missing syncType')
        }
        deps.logger.debug('skipping history sync notification without syncType')
        return
    }
    if (!HANDLED_SYNC_TYPES.has(syncType)) {
        if (boundaryContext) {
            throw new Error('history boundary notification has an unsupported syncType')
        }
        deps.logger.debug('skipping unhandled history sync type', { syncType })
        if (syncType === proto.Message.HistorySyncType.INITIAL_STATUS_V3 && deps.onProcessed) {
            await deps.onProcessed(syncType)
        }
        return
    }

    const blob = await openHistoryBlobStream(
        deps.mediaTransfer,
        notification,
        'history',
        'history sync',
        notification.initialHistBootstrapInlinePayload,
        boundaryContext
            ? {
                  signal: boundaryContext.signal,
                  maxEncryptedBytes: boundaryContext.limits.maxBatchBytes
              }
            : undefined
    )

    const state: HistorySyncChunkState = {
        writeBehind: deps.writeBehind,
        maxOutstandingHistoryWrites: boundaryContext
            ? Math.min(
                  Number.MAX_SAFE_INTEGER,
                  Math.max(
                      4_096,
                      (boundaryContext.limits.maxSourceRecordsPerBatch +
                          boundaryContext.limits.maxMessagesPerBatch +
                          2) *
                          boundaryContext.limits.maxConcurrentBatches +
                          4_096
                  )
              )
            : 4_096,
        nowMs: Date.now(),
        pendingWrites: [],
        pushnames: [],
        inlineContacts: [],
        pnToLid: new Map<string, string>(),
        tokenConversations: [],
        ...(boundaryContext
            ? {
                  boundary: {
                      context: boundaryContext,
                      notification,
                      observations: [],
                      familyDispositions: new Map(),
                      metadataConflicts: new Set(),
                      deferredMessageWrites: [],
                      blockedReason: null,
                      decodedSyncType: null,
                      sourceRecordCount: 0
                  }
              }
            : {}),
        boundaryParkedRecords: [],
        nctSalt: null,
        chunkOrder: null,
        progress: null,
        conversationsCount: 0,
        messagesCount: 0
    }

    let phase: HistoryFailureReason = 'WALK_FAILED'
    try {
        throwIfAborted(boundaryContext?.signal)
        await consumeHistorySyncStream(deps, blob.inflated, state)

        phase = 'VERIFICATION_REJECTED'
        await abortable(
            blob.verified.then(() => undefined),
            boundaryContext?.signal
        )

        if (state.boundary) {
            throwIfAborted(state.boundary.context.signal)
            if (state.boundary.blockedReason) {
                throw markHistoryFailure(
                    state.boundary.blockedReason,
                    'WALK_FAILED',
                    'POST_EOF_GATE_REJECTED'
                )
            }
            phase = 'BATCH_PROJECTION_FAILED'
            const batch = {
                version: 1 as const,
                sourceHistoryCompleteness: 'UNKNOWN' as const,
                notification: {
                    syncType,
                    ...(notification.chunkOrder !== null && notification.chunkOrder !== undefined
                        ? { chunkOrder: notification.chunkOrder }
                        : {}),
                    ...(notification.progress !== null && notification.progress !== undefined
                        ? { progress: notification.progress }
                        : {}),
                    ...(notification.originalMessageId
                        ? { originalMessageId: notification.originalMessageId }
                        : {}),
                    ...(notification.oldestMsgInChunkTimestampSec !== null &&
                    notification.oldestMsgInChunkTimestampSec !== undefined
                        ? {
                              oldestMsgInChunkTimestampSec: longToString(
                                  notification.oldestMsgInChunkTimestampSec
                              )
                          }
                        : {})
                },
                decoded: {
                    ...(state.boundary.decodedSyncType !== null
                        ? { syncType: state.boundary.decodedSyncType }
                        : {}),
                    ...(state.chunkOrder !== null ? { chunkOrder: state.chunkOrder } : {}),
                    ...(state.progress !== null ? { progress: state.progress } : {})
                },
                parseReachedEof: true as const,
                payloadIntegrityVerified: true as const,
                sourceFamilyDispositions: toFamilyDispositions(state.boundary),
                metadataConflicts: [...state.boundary.metadataConflicts].sort(),
                messageObservations: state.boundary.observations
            }
            phase = 'CALLBACK_REJECTED'
            await invokeBoundaryCallback(state.boundary.context, batch)
            phase = 'PRIVATE_SETTLEMENT_REJECTED'
            for (const persist of state.boundary.deferredMessageWrites) {
                pushWrite(state, persist)
            }
        }

        phase = 'PRIVATE_SETTLEMENT_REJECTED'
        await settleHistorySyncChunk(deps, state)

        deps.logger.info('decoded history sync chunk', {
            syncType,
            chunkOrder: state.chunkOrder,
            progress: state.progress,
            conversations: state.conversationsCount,
            messages: state.messagesCount,
            pushnames: state.pushnames.length,
            inlineContacts: state.inlineContacts.length
        })

        const event: WaHistorySyncChunkEvent = {
            syncType,
            messagesCount: state.messagesCount,
            conversationsCount: state.conversationsCount,
            pushnamesCount: state.pushnames.length,
            inlineContactsCount: state.inlineContacts.length,
            chunkOrder: state.chunkOrder ?? undefined,
            progress: state.progress ?? undefined
        }
        await abortable(flushPendingWrites(state.pendingWrites), state.boundary?.context.signal)
        deps.emitEvent('history_sync_chunk', event)
        if (deps.onProcessed) {
            await deps.onProcessed(syncType)
        }
    } catch (caught) {
        const error = markHistoryFailure(
            toError(caught),
            phase,
            phase === 'WALK_FAILED'
                ? 'WALK_FAILED'
                : phase === 'BATCH_PROJECTION_FAILED'
                  ? 'BATCH_PROJECTION_FAILED'
                  : undefined
        )
        markHistoryFailure(error, phase, 'CLEANUP_CANCEL_REQUESTED')
        blob.cancel()
        // Non-cancellable provider work already has private ownership. Do not
        // attach an abandoned allSettled continuation for every failed job.
        markHistoryFailure(error, phase, 'CLEANUP_WRITES_WAIT')
        const writesSettled =
            state.pendingWrites.length === 0 ||
            state.writeBehind.historyOperationsSettled(state.pendingWrites)
        markHistoryFailure(
            error,
            phase,
            writesSettled ? 'CLEANUP_WRITES_SETTLED' : 'CLEANUP_WRITES_RETAINED'
        )
        state.pendingWrites.length = 0
        if (state.boundary) state.boundary.deferredMessageWrites.length = 0
        state.boundaryParkedRecords.length = 0
        markHistoryFailure(error, phase, 'CLEANUP_VERIFY_WAIT')
        const transferSettled = await waitHistoryCleanup(blob.settled, boundaryContext?.signal)
        markHistoryFailure(
            error,
            phase,
            transferSettled ? 'CLEANUP_VERIFY_SETTLED' : 'CLEANUP_ABORTED'
        )
        throw error
    }
}

async function consumeHistorySyncStream(
    deps: WaHistorySyncDeps,
    inflated: Parameters<typeof streamProtoFields>[0],
    state: HistorySyncChunkState
): Promise<void> {
    let headerParts: Uint8Array[] | null = null
    let threadJid: string | null = null
    let parked: ParkedHistoryMessage[] | null = null
    const validation: HistoryProtocolValidationContext | undefined = state.boundary
        ? {
              signal: state.boundary.context.signal,
              maxFieldBytes: Math.min(
                  HISTORY_SYNC_MAX_RECORD_BYTES,
                  state.boundary.context.limits.maxBatchBytes
              ),
              chargeField: () => {
                  const boundary = state.boundary!
                  boundary.sourceRecordCount += 1
                  if (
                      boundary.sourceRecordCount > boundary.context.limits.maxSourceRecordsPerBatch
                  ) {
                      throw new Error(
                          'history boundary batch exceeds configured source-record limit'
                      )
                  }
              }
          }
        : undefined

    const onEvent = async (event: ProtoStreamEvent): Promise<void> => {
        throwIfAborted(state.boundary?.context.signal)
        if (event.kind === PROTO_STREAM_EVENT_KINDS.FIELD && state.boundary) {
            validation!.chargeField()
        }
        if (event.kind === PROTO_STREAM_EVENT_KINDS.ENTER) {
            headerParts = []
            threadJid = null
            parked = null
            state.conversationsCount += 1
            state.boundaryParkedRecords.length = 0
            return undefined
        }

        if (event.kind === PROTO_STREAM_EVENT_KINDS.LEAVE) {
            const parts = headerParts
            const jid = threadJid
            const pending = parked
            const boundaryRecords = state.boundaryParkedRecords.splice(0)
            const conversationOrdinal = state.conversationsCount
            headerParts = null
            threadJid = null
            parked = null
            return parts
                ? closeConversation(
                      deps,
                      state,
                      parts,
                      jid,
                      pending,
                      boundaryRecords,
                      conversationOrdinal
                  )
                : undefined
        }

        if (event.depth === 1) {
            recordConversationField(state, event.fieldNumber, event.wireType)
            if (
                event.fieldNumber === CONVERSATION_FIELDS.MESSAGES &&
                event.wireType === PROTO_WIRE_TYPES.LEN
            ) {
                let recordBytes = event.value
                if (validation) {
                    const prepared = await prepareBoundaryHistoryRecord(recordBytes, validation)
                    recordBytes = prepared.bytes
                    for (const [scope, count] of [
                        ['MESSAGE_EXTENDED_TEXT', prepared.outerCount],
                        ['QUOTED_MESSAGE_EXTENDED_TEXT', prepared.quotedCount]
                    ] as const) {
                        if (count) {
                            recordBoundaryFamily(
                                state,
                                scope,
                                4,
                                'EXCLUDED_BY_ACCEPTED_SCOPE',
                                count
                            )
                        }
                    }
                }
                if (state.boundary) {
                    if (
                        state.boundary.observations.length + state.boundaryParkedRecords.length >=
                        state.boundary.context.limits.maxMessagesPerBatch
                    ) {
                        throw new Error('history boundary batch exceeds configured message limit')
                    }
                    if (!threadJid || state.boundaryParkedRecords.length > 0) {
                        state.boundaryParkedRecords.push(recordBytes.slice())
                    } else {
                        captureBoundaryMessage(
                            state,
                            recordBytes,
                            threadJid,
                            state.conversationsCount
                        )
                    }
                }
                const message = readHistoryMessage(recordBytes)
                if (!message) {
                    return undefined
                }
                if (!threadJid) {
                    parked ??= []
                    if (parked.length >= HISTORY_SYNC_MAX_PARKED_MESSAGES) {
                        throw new Error(
                            `history sync parked ${parked.length} messages with no thread jid, exceeding the ${HISTORY_SYNC_MAX_PARKED_MESSAGES} limit`
                        )
                    }
                    parked[parked.length] = message
                    return undefined
                }
                persistHistoryMessage(deps, state, message, threadJid)
                return maybeFlush(state)
            }

            if (headerParts) {
                if (
                    event.fieldNumber === CONVERSATION_FIELDS.ID &&
                    event.wireType === PROTO_WIRE_TYPES.LEN
                ) {
                    threadJid = TEXT_DECODER.decode(event.value)
                }
                headerParts[headerParts.length] = event.raw.slice()
            }
            return undefined
        }

        if (event.depth !== 0) {
            return undefined
        }

        if (event.kind === PROTO_STREAM_EVENT_KINDS.FIELD && state.boundary) {
            if ([8, 9, 10, 12, 13].includes(event.fieldNumber)) {
                await validateAcceptedHistoryRoot(
                    event.fieldNumber,
                    event.wireType,
                    event.raw,
                    validation!
                )
                recordBoundaryFamily(state, 'ROOT', event.fieldNumber, 'EXCLUDED_BY_ACCEPTED_SCOPE')
                return
            }
            recordRootFamily(state, event.fieldNumber, event.wireType)
            if (
                event.fieldNumber === HISTORY_SYNC_FIELDS.SYNC_TYPE &&
                event.varintValue !== state.boundary.notification.syncType
            ) {
                state.boundary.blockedReason ??= historyFailure(
                    'notification_payload_sync_type_mismatch',
                    'HistorySync',
                    1,
                    event.wireType
                )
            }
        }

        if (event.wireType === PROTO_WIRE_TYPES.VARINT) {
            if (event.fieldNumber === HISTORY_SYNC_FIELDS.CHUNK_ORDER) {
                state.chunkOrder = event.varintValue
                if (
                    state.boundary &&
                    state.boundary.notification.chunkOrder !== null &&
                    state.boundary.notification.chunkOrder !== undefined &&
                    state.boundary.notification.chunkOrder !== event.varintValue
                ) {
                    state.boundary.metadataConflicts.add(
                        'notification_payload_chunk_order_mismatch'
                    )
                }
            } else if (event.fieldNumber === HISTORY_SYNC_FIELDS.PROGRESS) {
                state.progress = event.varintValue
                if (
                    state.boundary &&
                    state.boundary.notification.progress !== null &&
                    state.boundary.notification.progress !== undefined &&
                    state.boundary.notification.progress !== event.varintValue
                ) {
                    state.boundary.metadataConflicts.add('notification_payload_progress_mismatch')
                }
            } else if (event.fieldNumber === 1 && state.boundary) {
                state.boundary.decodedSyncType = event.varintValue
            }
            return undefined
        }

        if (event.wireType !== PROTO_WIRE_TYPES.LEN) {
            return undefined
        }

        if (event.fieldNumber === HISTORY_SYNC_FIELDS.PUSHNAMES) {
            state.pushnames[state.pushnames.length] = proto.Pushname.decode(event.value)
        } else if (event.fieldNumber === HISTORY_SYNC_FIELDS.PHONE_NUMBER_TO_LID_MAPPINGS) {
            const map = proto.PhoneNumberToLIDMapping.decode(event.value)
            if (map.pnJid && map.lidJid) {
                state.pnToLid.set(map.pnJid, map.lidJid)
            }
        } else if (event.fieldNumber === HISTORY_SYNC_FIELDS.INLINE_CONTACTS) {
            const contact = proto.InlineContact.decode(event.value)
            state.inlineContacts[state.inlineContacts.length] = contact
            if (contact.pnJid && contact.lidJid) {
                state.pnToLid.set(contact.pnJid, contact.lidJid)
            }
        } else if (event.fieldNumber === HISTORY_SYNC_FIELDS.NCT_SALT) {
            state.nctSalt = event.value.slice()
        }
        return undefined
    }

    await streamProtoFields(inflated, onEvent, {
        rejectGroups: Boolean(state.boundary),
        shouldDescend: (fieldNumber, depth) =>
            depth === 0 && fieldNumber === HISTORY_SYNC_FIELDS.CONVERSATIONS,
        maxFieldBytes: state.boundary
            ? Math.min(HISTORY_SYNC_MAX_RECORD_BYTES, state.boundary.context.limits.maxBatchBytes)
            : HISTORY_SYNC_MAX_RECORD_BYTES,
        maxTotalBytes: state.boundary?.context.limits.maxBatchBytes
    })
}

async function closeConversation(
    deps: WaHistorySyncDeps,
    state: HistorySyncChunkState,
    headerParts: readonly Uint8Array[],
    threadJid: string | null,
    parked: readonly ParkedHistoryMessage[] | null,
    boundaryRecords: readonly Uint8Array[],
    conversationOrdinal: number
): Promise<void> {
    const conversation = proto.Conversation.decode(concatBytes(headerParts))
    const resolvedJid = threadJid ?? conversation.id
    if (!resolvedJid) {
        if (state.boundary) {
            state.boundary.blockedReason ??= historyFailure(
                'missing_conversation_address',
                'Conversation',
                1,
                null
            )
        }
        deps.logger.debug('skipping history sync conversation without thread jid')
        return
    }
    if (state.boundary) {
        if (conversation.$unknowns?.length) {
            state.boundary.blockedReason ??= unknownFieldFailure(
                'unknown_conversation_fields',
                'Conversation',
                conversation.$unknowns
            )
        }
        if (classifyHistoryChat(resolvedJid) === 'UNKNOWN') {
            state.boundary.blockedReason ??= historyFailure(
                'unclassified_conversation_address',
                'Conversation',
                1,
                2
            )
        }
        for (const record of boundaryRecords) {
            captureBoundaryMessage(state, record, resolvedJid, conversationOrdinal)
        }
    }

    if (conversation.pnJid) {
        const lidJid = conversation.lidJid ?? conversation.accountLid
        if (lidJid) {
            state.pnToLid.set(conversation.pnJid, lidJid)
        }
    }

    if (
        deps.onPrivacyTokens &&
        (conversation.tcToken ||
            conversation.tcTokenTimestamp ||
            conversation.tcTokenSenderTimestamp)
    ) {
        state.tokenConversations[state.tokenConversations.length] = {
            jid: resolvedJid,
            tcToken: conversation.tcToken ? new Uint8Array(conversation.tcToken) : undefined,
            tcTokenTimestamp: longToNumber(conversation.tcTokenTimestamp) || undefined,
            tcTokenSenderTimestamp: longToNumber(conversation.tcTokenSenderTimestamp) || undefined
        }
    }

    const ephemeralExpiration = conversation.ephemeralExpiration ?? undefined
    const ephemeralSettingTimestamp =
        normalizeEphemeralSettingSeconds(longToNumber(conversation.ephemeralSettingTimestamp)) ||
        undefined
    if (deps.chatMetadataStore && ephemeralExpiration !== undefined) {
        const boundaryMode = Boolean(state.boundary)
        pushWrite(state, () =>
            deps
                .chatMetadataStore!.upsertChatMetadata({
                    chatJid: resolvedJid,
                    ephemeralExpiration,
                    ...(ephemeralSettingTimestamp !== undefined
                        ? { ephemeralSettingTimestamp }
                        : {}),
                    updatedAtMs: state.nowMs
                })
                .catch((error: unknown) => {
                    if (boundaryMode) {
                        deps.logger.debug('failed to cache history sync chat metadata')
                        return
                    }
                    deps.logger.debug('failed to cache history sync chat metadata', {
                        jid: resolvedJid,
                        message: toError(error).message
                    })
                })
        )
    }
    pushWrite(state, () =>
        deps.writeBehind.persistThreadAsync({
            jid: resolvedJid,
            name: conversation.name ?? undefined,
            unreadCount: conversation.unreadCount ?? undefined,
            archived: conversation.archived ?? undefined,
            pinned: conversation.pinned ?? undefined,
            muteEndMs: longToNumber(conversation.muteEndTime) || undefined,
            markedAsUnread: conversation.markedAsUnread ?? undefined,
            ephemeralExpiration,
            ephemeralSettingTimestamp
        })
    )

    if (isUserJid(resolvedJid) || (conversation.lidJid ?? conversation.accountLid)) {
        const contactDisplay = conversation.displayName || undefined
        const contactUsername = conversation.username
            ? normalizeUsername(conversation.username)
            : undefined
        const contactPn = conversation.pnJid ?? undefined
        const contactLid = conversation.lidJid ?? conversation.accountLid ?? undefined
        if (contactDisplay || contactUsername || contactPn || contactLid) {
            pushWrite(state, () =>
                deps.writeBehind.persistContactAsync({
                    jid: contactLid ?? resolvedJid,
                    displayName: contactDisplay,
                    username: contactUsername,
                    phoneNumber: contactPn,
                    lastUpdatedMs: state.nowMs
                })
            )
        }
    }

    for (const message of parked ?? []) {
        persistHistoryMessage(deps, state, message, resolvedJid)
    }

    await maybeFlush(state)
}

/**
 * Resolves the author here rather than at decode time because a message may be
 * read before its `Conversation.id`, and the thread type decides both the
 * self-sent fallback and the shape of the record.
 *
 * `participantJid` carries the group/broadcast author and stays absent in 1:1
 * threads, where `senderJid` falls back to the thread JID - the same shape the
 * live message path persists. A group author that stays unresolved leaves both
 * fields empty: writing the group JID there would read downstream as if the
 * group itself had sent the message.
 */
function persistHistoryMessage(
    deps: WaHistorySyncDeps,
    state: HistorySyncChunkState,
    message: ParkedHistoryMessage,
    threadJid: string
): void {
    state.messagesCount += 1
    const authorJid = resolveWebMessageInfoAuthor(message.authorInfo, deps.meJid, threadJid)
    const isGroupOrBroadcast = isGroupJid(threadJid) || isBroadcastJid(threadJid)
    const persist = (): Promise<void> =>
        deps.writeBehind.persistMessageAsync({
            id: message.id,
            threadJid,
            senderJid: isGroupOrBroadcast ? authorJid : (authorJid ?? threadJid),
            participantJid: isGroupOrBroadcast ? authorJid : undefined,
            fromMe: message.fromMe,
            timestampMs: message.timestampMs,
            messageBytes: message.messageBytes
        })
    if (state.boundary) {
        state.boundary.deferredMessageWrites.push(persist)
    } else {
        pushWrite(state, persist)
    }
}

async function settleHistorySyncChunk(
    deps: WaHistorySyncDeps,
    state: HistorySyncChunkState
): Promise<void> {
    for (const pushname of state.pushnames) {
        if (!pushname.id) {
            continue
        }
        const pushnameId = pushname.id
        const lidJid = state.pnToLid.get(pushnameId)
        pushWrite(state, () =>
            deps.writeBehind.persistContactAsync(
                lidJid
                    ? {
                          jid: lidJid,
                          pushName: pushname.pushname ?? undefined,
                          phoneNumber: pushnameId,
                          lastUpdatedMs: state.nowMs
                      }
                    : {
                          jid: pushnameId,
                          pushName: pushname.pushname ?? undefined,
                          lastUpdatedMs: state.nowMs
                      }
            )
        )
        await maybeFlush(state)
    }

    for (const contact of state.inlineContacts) {
        const jid = contact.lidJid ?? contact.pnJid
        if (!jid) {
            continue
        }
        const displayName = contact.fullName || contact.firstName || undefined
        if (!displayName && !contact.pnJid) {
            continue
        }
        pushWrite(state, () =>
            deps.writeBehind.persistContactAsync({
                jid,
                displayName,
                phoneNumber: contact.pnJid ?? undefined,
                lastUpdatedMs: state.nowMs
            })
        )
        await maybeFlush(state)
    }

    for (const [pnJid, lidJid] of state.pnToLid) {
        pushWrite(state, () =>
            deps.writeBehind.persistContactAsync({
                jid: lidJid,
                phoneNumber: pnJid,
                lastUpdatedMs: state.nowMs
            })
        )
        await maybeFlush(state)
    }

    if (deps.onPrivacyTokens && state.tokenConversations.length > 0) {
        pushWrite(state, () => deps.onPrivacyTokens!(state.tokenConversations))
    }
    if (deps.onNctSalt && state.nctSalt) {
        pushWrite(state, () => deps.onNctSalt!(state.nctSalt!))
    }
}

/** `null` for content-less stubs, which duplicate live notification stanzas. */
function readHistoryMessage(record: Uint8Array): ParkedHistoryMessage | null {
    const webMsg = proto.HistorySyncMsg.decode(record).message
    if (!webMsg?.key?.id || !webMsg.message) {
        return null
    }
    const timestampMs = longToNumber(webMsg.messageTimestamp) * 1000
    return {
        id: webMsg.key.id,
        authorInfo: {
            key: { fromMe: webMsg.key.fromMe, participant: webMsg.key.participant },
            participant: webMsg.participant,
            originalSelfAuthorUserJidString: webMsg.originalSelfAuthorUserJidString
        },
        fromMe: webMsg.key.fromMe === true,
        timestampMs: timestampMs || undefined,
        messageBytes: proto.Message.encode(webMsg.message).finish()
    }
}

function captureBoundaryMessage(
    state: HistorySyncChunkState,
    recordBytes: Uint8Array,
    conversationAddress: string,
    conversationOrdinal: number
): void {
    const boundary = state.boundary
    if (!boundary) return
    const record = proto.HistorySyncMsg.decode(recordBytes)
    const message = record.message
    recordBoundaryFamily(state, 'HISTORY_SYNC_MESSAGE', 1, 'MESSAGE_OBSERVATIONS')
    if (record.msgOrderId !== null && record.msgOrderId !== undefined) {
        recordBoundaryFamily(state, 'HISTORY_SYNC_MESSAGE', 2, 'METADATA_ONLY')
    }
    if (record.$unknowns?.length || !message || message.$unknowns?.length) {
        boundary.blockedReason ??= record.$unknowns?.length
            ? unknownFieldFailure(
                  'unknown_or_missing_message_record',
                  'HistorySyncMsg',
                  record.$unknowns
              )
            : message?.$unknowns?.length
              ? unknownFieldFailure(
                    'unknown_or_missing_message_record',
                    'WebMessageInfo',
                    message.$unknowns
                )
              : historyFailure('unknown_or_missing_message_record', 'HistorySyncMsg', 1, null)
        return
    }
    recordBoundaryFamily(state, 'WEB_MESSAGE_INFO', 1, 'MESSAGE_OBSERVATIONS')
    if (message.key?.$unknowns?.length || message.message?.$unknowns?.length) {
        boundary.blockedReason ??= message.key?.$unknowns?.length
            ? unknownFieldFailure('unknown_message_fields', 'MessageKey', message.key.$unknowns)
            : unknownFieldFailure('unknown_message_fields', 'Message', message.message?.$unknowns)
        return
    }

    const chatKind = classifyHistoryChat(conversationAddress)
    if (chatKind === 'UNKNOWN') {
        boundary.blockedReason ??= historyFailure(
            'unclassified_conversation_address',
            'Conversation',
            1,
            2
        )
        return
    }

    const isDirect = chatKind === 'DIRECT'
    const messageId = message.key?.id ?? undefined
    if (
        isDirect &&
        (!messageId ||
            !message.message ||
            message.key?.fromMe === null ||
            message.key?.fromMe === undefined)
    ) {
        boundary.blockedReason ??= historyFailure(
            'direct_message_missing_required_identity_or_content',
            !message.message ? 'WebMessageInfo' : 'MessageKey',
            !message.message ? 2 : !messageId ? 3 : 2,
            null
        )
        return
    }
    if (
        isDirect &&
        message.key?.remoteJid !== null &&
        message.key?.remoteJid !== undefined &&
        message.key.remoteJid !== conversationAddress
    ) {
        boundary.blockedReason ??= historyFailure(
            'direct_message_address_conflict',
            'MessageKey',
            1,
            2
        )
        return
    }

    let content: WaHistorySyncMessageObservation['content'] = { kind: 'UNSUPPORTED' }
    if (isDirect && message.message) {
        const contentType = getContentType(message.message)
        if (!contentType) {
            boundary.blockedReason ??= historyFailure(
                'direct_message_content_unclassified',
                'Message'
            )
            return
        }
        if (
            contentType === 'extendedTextMessage' &&
            hasUnknownFieldsDeep(message.message.extendedTextMessage)
        ) {
            boundary.blockedReason ??= message.message.extendedTextMessage?.$unknowns?.length
                ? unknownFieldFailure(
                      'unknown_extended_text_fields',
                      'Message.ExtendedTextMessage',
                      message.message.extendedTextMessage.$unknowns
                  )
                : historyFailure('unknown_extended_text_fields')
            return
        }
        if (contentType === 'conversation' && typeof message.message.conversation === 'string') {
            content = { kind: 'TEXT', text: message.message.conversation }
        } else if (
            contentType === 'extendedTextMessage' &&
            typeof message.message.extendedTextMessage?.text === 'string'
        ) {
            content = { kind: 'TEXT', text: message.message.extendedTextMessage.text }
        } else {
            content = { kind: 'UNSUPPORTED', kindHint: String(contentType) }
        }
    }

    const observation: WaHistorySyncMessageObservation = {
        ordinal: boundary.observations.length + 1,
        conversationOrdinal,
        chatKind,
        disposition:
            chatKind === 'DIRECT'
                ? 'IMPORT_DIRECT'
                : chatKind === 'GROUP'
                  ? 'EXCLUDED_GROUP'
                  : chatKind === 'BROADCAST'
                    ? 'EXCLUDED_BROADCAST'
                    : 'EXCLUDED_NEWSLETTER',
        ...(isDirect && messageId ? { externalMessageId: messageId } : {}),
        ...(isDirect ? { conversationAddress } : {}),
        ...(isDirect && message.key?.remoteJid ? { messageRemoteJid: message.key.remoteJid } : {}),
        ...(isDirect && message.key?.fromMe !== null && message.key?.fromMe !== undefined
            ? { fromMe: message.key.fromMe }
            : {}),
        ...(isDirect && message.messageTimestamp !== null && message.messageTimestamp !== undefined
            ? { timestampSeconds: longToString(message.messageTimestamp) }
            : {}),
        ...(isDirect && record.msgOrderId !== null && record.msgOrderId !== undefined
            ? { msgOrderId: longToString(record.msgOrderId) }
            : {}),
        ...(isDirect && message.messageStubType !== null && message.messageStubType !== undefined
            ? { messageStubType: message.messageStubType }
            : {}),
        content
    }
    boundary.observations.push(observation)
}

function hasUnknownFieldsDeep(value: unknown, visited = new WeakSet<object>()): boolean {
    if (value === null || typeof value !== 'object' || value instanceof Uint8Array) {
        return false
    }
    if (visited.has(value)) return false
    visited.add(value)

    const message = value as { readonly $unknowns?: unknown }
    if (Array.isArray(message.$unknowns) && message.$unknowns.length > 0) {
        return true
    }

    const children = Array.isArray(value) ? value : Object.values(value)
    return children.some((child) => hasUnknownFieldsDeep(child, visited))
}

/** Inspect only the first structural tag, never copy or retain an unknown field's value. */
function unknownFieldFailure(
    reason: HistoryFailureReason,
    type: string,
    unknowns: readonly Uint8Array[] | undefined
): Error {
    const bytes = unknowns?.[0]
    let tag = 0
    if (bytes) {
        for (let index = 0; index < Math.min(5, bytes.length); index += 1) {
            tag += (bytes[index] & 0x7f) * 2 ** (7 * index)
            if (!(bytes[index] & 0x80)) {
                if (tag >= 8 && tag <= 0xffff_ffff) {
                    return historyFailure(reason, type, Math.floor(tag / 8), tag % 8)
                }
                break
            }
        }
    }
    return historyFailure(reason, type)
}

function classifyHistoryChat(address: string): WaHistorySyncChatKind {
    if (isGroupJid(address)) return 'GROUP'
    if (isBroadcastJid(address)) return 'BROADCAST'
    if (isNewsletterJid(address)) return 'NEWSLETTER'
    if (isUserJid(address) || isLidJid(address)) return 'DIRECT'
    try {
        if (isHostedServer(splitJid(address).server)) return 'DIRECT'
    } catch {
        return 'UNKNOWN'
    }
    return 'UNKNOWN'
}

function recordRootFamily(
    state: HistorySyncChunkState,
    fieldNumber: number,
    wireType: number
): void {
    const boundary = state.boundary
    if (!boundary) return
    const disposition = rootFamilyDisposition(fieldNumber)
    recordBoundaryFamily(state, 'ROOT', fieldNumber, disposition)
    const expectedWire = rootFamilyWireType(fieldNumber)
    if (disposition === 'BLOCKED_UNDECIDED') {
        boundary.blockedReason ??= historyFailure(
            'unsupported_history_root_family',
            'HistorySync',
            fieldNumber,
            wireType
        )
    } else if (wireType !== expectedWire) {
        boundary.blockedReason ??= historyFailure(
            'malformed_history_root_family',
            'HistorySync',
            fieldNumber,
            wireType
        )
    }
}

function recordBoundaryFamily(
    state: HistorySyncChunkState,
    scope: WaHistorySyncFamilyDisposition['scope'],
    fieldNumber: number,
    disposition: WaHistorySyncFamilyDisposition['disposition'],
    count = 1
): void {
    const boundary = state.boundary
    if (!boundary) return
    const key = `${scope}:${fieldNumber}`
    const existing = boundary.familyDispositions.get(key)
    boundary.familyDispositions.set(key, {
        scope,
        fieldNumber,
        disposition,
        count: (existing?.count ?? 0) + count
    })
}

function rootFamilyDisposition(fieldNumber: number): WaHistorySyncFamilyDisposition['disposition'] {
    if ([8, 9, 10, 12, 13].includes(fieldNumber)) return 'EXCLUDED_BY_ACCEPTED_SCOPE'
    if (fieldNumber === HISTORY_SYNC_FIELDS.CONVERSATIONS) return 'MESSAGE_OBSERVATIONS'
    if ([1, 5, 6, 7, 15, 19, 20, 21].includes(fieldNumber)) return 'METADATA_ONLY'
    return 'BLOCKED_UNDECIDED'
}

function rootFamilyWireType(fieldNumber: number): number {
    return [1, 5, 6, 10, 21].includes(fieldNumber) ? PROTO_WIRE_TYPES.VARINT : PROTO_WIRE_TYPES.LEN
}

function recordConversationField(
    state: HistorySyncChunkState,
    fieldNumber: number,
    wireType: number
): void {
    const boundary = state.boundary
    if (!boundary) return
    if (fieldNumber < 1 || fieldNumber > 63) {
        boundary.blockedReason ??= historyFailure(
            'unknown_conversation_field',
            'Conversation',
            fieldNumber,
            wireType
        )
        return
    }
    recordBoundaryFamily(
        state,
        'CONVERSATION',
        fieldNumber,
        fieldNumber === CONVERSATION_FIELDS.ID || fieldNumber === CONVERSATION_FIELDS.MESSAGES
            ? 'MESSAGE_OBSERVATIONS'
            : 'METADATA_ONLY'
    )
    if (
        (fieldNumber === CONVERSATION_FIELDS.ID || fieldNumber === CONVERSATION_FIELDS.MESSAGES) &&
        wireType !== PROTO_WIRE_TYPES.LEN
    ) {
        boundary.blockedReason ??= historyFailure(
            'malformed_conversation_field',
            'Conversation',
            fieldNumber,
            wireType
        )
    }
}

function toFamilyDispositions(boundary: HistoryBoundaryCapture): WaHistorySyncFamilyDisposition[] {
    return [...boundary.familyDispositions.values()].sort(
        (left, right) =>
            left.scope.localeCompare(right.scope) || left.fieldNumber - right.fieldNumber
    )
}

async function invokeBoundaryCallback(
    context: HistoryBoundaryMessageContext,
    batch: Parameters<WaHistorySyncBatchHandler>[0]
): Promise<void> {
    throwIfAborted(context.signal)
    await abortable(
        Promise.resolve().then(() => {
            throwIfAborted(context.signal)
            return context.onBatch(batch, { signal: context.signal })
        }),
        context.signal
    )
}

function throwIfAborted(signal: AbortSignal | undefined): void {
    if (signal?.aborted) throw markHistoryFailure(abortError(signal), 'CANCELLED')
}

function abortError(signal: AbortSignal): Error {
    return signal.reason instanceof Error
        ? signal.reason
        : new Error('history boundary was cancelled')
}

function abortable<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
    if (!signal) return promise
    if (signal.aborted) {
        promise.catch(() => undefined)
        return Promise.reject(markHistoryFailure(abortError(signal), 'CANCELLED'))
    }
    return new Promise<T>((resolve, reject) => {
        const onAbort = (): void => {
            signal.removeEventListener('abort', onAbort)
            reject(markHistoryFailure(abortError(signal), 'CANCELLED'))
        }
        signal.addEventListener('abort', onAbort, { once: true })
        promise.then(resolve, reject).finally(() => {
            signal.removeEventListener('abort', onAbort)
        })
    })
}

function longToString(value: number | { toString(): string }): string {
    return typeof value === 'number' ? String(value) : value.toString()
}

/** The no-op handler keeps a queued rejection observed until the flush rejects on it. */
function pushWrite(state: HistorySyncChunkState, start: () => Promise<void>): void {
    const write = state.writeBehind.ownHistoryOperation(start, state.maxOutstandingHistoryWrites)
    write.catch(() => undefined)
    state.pendingWrites[state.pendingWrites.length] = write
}

function maybeFlush(state: HistorySyncChunkState): Promise<void> | undefined {
    if (state.pendingWrites.length >= HISTORY_SYNC_MAX_PENDING_WRITES) {
        return abortable(flushPendingWrites(state.pendingWrites), state.boundary?.context.signal)
    }
    return undefined
}
