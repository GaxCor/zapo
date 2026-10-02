/** Internal fixed provenance only; never project exception text or protocol values. */
export const HISTORY_FAILURE_REASONS = Object.freeze([
    'WALK_FAILED',
    'CANCELLED',
    'VERIFICATION_REJECTED',
    'BATCH_PROJECTION_FAILED',
    'CALLBACK_ADMISSION_REJECTED',
    'CALLBACK_REJECTED',
    'PRIVATE_SETTLEMENT_REJECTED',
    'notification_payload_sync_type_mismatch',
    'missing_conversation_address',
    'unknown_conversation_fields',
    'unclassified_conversation_address',
    'unknown_or_missing_message_record',
    'unknown_message_fields',
    'direct_message_missing_required_identity_or_content',
    'direct_message_address_conflict',
    'direct_message_content_unclassified',
    'unknown_extended_text_fields',
    'unsupported_history_root_family',
    'malformed_history_root_family',
    'unknown_conversation_field',
    'malformed_conversation_field',
    'history_protocol_cancelled',
    'malformed_history_protocol_varint',
    'malformed_history_protocol_tag',
    'malformed_or_oversized_history_protocol_length',
    'malformed_history_protocol_fixed',
    'unsupported_history_protocol_wire',
    'malformed_history_protocol_utf8',
    'invalid_history_protocol_enum',
    'invalid_history_protocol_uint32',
    'invalid_history_protocol_bool',
    'invalid_history_protocol_int32',
    'invalid_history_group_address',
    'conflicting_history_group_address',
    'history_protocol_depth_limit',
    'unsupported_history_protocol_type',
    'malformed_history_protocol_field_wire',
    'malformed_history_protocol_packed',
    'history_protocol_projection_invariant',
    'history_protocol_record_limit'
] as const)

export type HistoryFailureReason = (typeof HISTORY_FAILURE_REASONS)[number]
export type HistoryCleanupStage =
    | 'WALK_FAILED'
    | 'CLEANUP_CANCEL_REQUESTED'
    | 'CLEANUP_WRITES_WAIT'
    | 'CLEANUP_WRITES_SETTLED'
    | 'CLEANUP_WRITES_RETAINED'
    | 'CLEANUP_VERIFY_WAIT'
    | 'CLEANUP_VERIFY_SETTLED'
    | 'CLEANUP_ABORTED'
    | 'POST_EOF_GATE_REJECTED'
    | 'BATCH_PROJECTION_FAILED'
    | 'CALLBACK_ADMISSION_REJECTED'

export interface HistoryFailureDiagnostic {
    readonly reason: HistoryFailureReason
    readonly type: string | null
    readonly field: number | null
    readonly wire: number | null
}

interface FailureRecord {
    diagnostic: HistoryFailureDiagnostic
    readonly stages: Set<HistoryCleanupStage>
}

const failures = new WeakMap<Error, FailureRecord>()

/** Called only with pinned source type tokens, never protocol-provided names. */
export function historyFailure(
    reason: HistoryFailureReason,
    type: string | null = null,
    field: number | null = null,
    wire: number | null = null
): Error {
    const error = new Error(reason)
    failures.set(error, {
        diagnostic: Object.freeze({ reason, type, field, wire }),
        stages: new Set()
    })
    return error
}

/** Preserve rejection identity and the first fixed failure, without inspecting its text. */
export function markHistoryFailure(
    error: Error,
    reason: HistoryFailureReason,
    stage?: HistoryCleanupStage
): Error {
    if (!failures.has(error)) {
        failures.set(error, {
            diagnostic: Object.freeze({ reason, type: null, field: null, wire: null }),
            stages: new Set()
        })
    }
    if (stage) failures.get(error)!.stages.add(stage)
    return error
}

/** Innermost identified field wins; framing failures may legitimately have null provenance. */
export function identifyHistoryFailure(
    error: unknown,
    type: string,
    field: number | null,
    wire: number | null
): void {
    if (!(error instanceof Error)) return
    const record = failures.get(error)
    if (record && record.diagnostic.type === null) {
        record.diagnostic = Object.freeze({ ...record.diagnostic, type, field, wire })
    }
}

/** Internal test/diagnostic projection: no Error, exception text, stack or values escape. */
export function getHistoryFailure(error: unknown): {
    readonly diagnostic: HistoryFailureDiagnostic
    readonly stages: readonly HistoryCleanupStage[]
} | null {
    if (!(error instanceof Error)) return null
    const record = failures.get(error)
    return record ? { diagnostic: record.diagnostic, stages: [...record.stages] } : null
}

/** Bound only the cleanup wait. Cancellation/underlying-work ownership belongs to the caller. */
export function waitHistoryCleanup(
    settled: Promise<unknown>,
    signal?: AbortSignal,
    timeoutMs = 100
): Promise<boolean> {
    return new Promise((resolve) => {
        let done = false
        const finish = (completed: boolean): void => {
            if (done) return
            done = true
            clearTimeout(timer)
            signal?.removeEventListener('abort', onAbort)
            resolve(completed)
        }
        const onAbort = (): void => finish(false)
        const timer = setTimeout(onAbort, timeoutMs)
        signal?.addEventListener('abort', onAbort, { once: true })
        // Both handlers stay attached to observe late true settlement, not cancel it.
        settled.then(
            () => finish(true),
            () => finish(true)
        )
        if (signal?.aborted) finish(false)
    })
}
