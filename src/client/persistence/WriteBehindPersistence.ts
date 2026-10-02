import type { WaWriteBehindOptions } from '@client/types'
import type { Logger } from '@infra/log/types'
import { BackgroundQueue, type BackgroundQueueFlushResult } from '@infra/perf/BackgroundQueue'
import type { WaContactStore, WaStoredContactRecord } from '@store/contracts/contact.store'
import type { WaMessageStore, WaStoredMessageRecord } from '@store/contracts/message.store'
import type { WaStoredThreadRecord, WaThreadStore } from '@store/contracts/thread.store'
import { toError } from '@util/primitives'

interface WriteBehindStores {
    readonly messageStore: WaMessageStore
    readonly threadStore: WaThreadStore
    readonly contactStore: WaContactStore
}

export interface WriteBehindDrainResult {
    readonly messages: BackgroundQueueFlushResult
    readonly threads: BackgroundQueueFlushResult
    readonly contacts: BackgroundQueueFlushResult
    readonly history: BackgroundQueueFlushResult
    readonly flushed: number
    readonly remaining: number
}

function mergeThread(
    previous: WaStoredThreadRecord,
    incoming: WaStoredThreadRecord
): WaStoredThreadRecord {
    return {
        jid: incoming.jid,
        name: incoming.name ?? previous.name,
        unreadCount: incoming.unreadCount ?? previous.unreadCount,
        archived: incoming.archived ?? previous.archived,
        pinned: incoming.pinned ?? previous.pinned,
        muteEndMs: incoming.muteEndMs ?? previous.muteEndMs,
        markedAsUnread: incoming.markedAsUnread ?? previous.markedAsUnread,
        ephemeralExpiration: incoming.ephemeralExpiration ?? previous.ephemeralExpiration,
        ephemeralSettingTimestamp:
            incoming.ephemeralSettingTimestamp ?? previous.ephemeralSettingTimestamp
    }
}

function mergeContact(
    previous: WaStoredContactRecord,
    incoming: WaStoredContactRecord
): WaStoredContactRecord {
    return {
        jid: incoming.jid,
        displayName: incoming.displayName ?? previous.displayName,
        pushName: incoming.pushName ?? previous.pushName,
        lid: incoming.lid ?? previous.lid,
        phoneNumber: incoming.phoneNumber ?? previous.phoneNumber,
        username: incoming.username ?? previous.username,
        lastUpdatedMs: Math.max(previous.lastUpdatedMs, incoming.lastUpdatedMs)
    }
}

export class WriteBehindPersistence {
    private readonly logger: Logger
    private readonly stores: WriteBehindStores
    private readonly options: WaWriteBehindOptions
    private queues: {
        readonly messages: BackgroundQueue<string, WaStoredMessageRecord>
        readonly threads: BackgroundQueue<string, WaStoredThreadRecord>
        readonly contacts: BackgroundQueue<string, WaStoredContactRecord>
    }
    private readonly flushTimeoutMs: number
    private destroyed: boolean
    private readonly historyOperations = new Map<Promise<void>, number>()
    private historyOutstanding = 0
    private readonly historyDrainWaiters = new Set<() => void>()
    private historySettled = 0

    /**
     * Internal history ownership. Admission is checked before starting work so
     * repeated failed jobs cannot create unbounded non-cancellable provider work.
     * No live consumer is serialized behind this tracker.
     */
    public ownHistoryOperation(start: () => Promise<void>, maxOutstanding = 4_096): Promise<void> {
        if (
            !Number.isSafeInteger(maxOutstanding) ||
            maxOutstanding < 1 ||
            this.destroyed ||
            this.historyOutstanding >= maxOutstanding
        ) {
            throw new Error('history private operation admission rejected')
        }
        const operation = start()
        const previous = this.historyOperations.get(operation) ?? 0
        this.historyOperations.set(operation, previous + 1)
        this.historyOutstanding += 1
        // A provider may share one promise across several started operations.
        // Bound operation count but attach only one tracker observer per promise.
        if (previous > 0) return operation
        const settled = (): void => {
            const count = this.historyOperations.get(operation)
            if (count === undefined) return
            this.historyOperations.delete(operation)
            this.historyOutstanding -= count
            this.historySettled += count
            if (this.historyOperations.size === 0) {
                for (const notify of this.historyDrainWaiters) notify()
            }
        }
        // Keep ownership and a rejection observer until the provider truly settles.
        operation.then(settled, settled)
        return operation
    }

    /** Internal inspection for failure cleanup, with no new promise continuations. */
    public historyOperationsSettled(operations: readonly Promise<void>[]): boolean {
        return operations.every((operation) => !this.historyOperations.has(operation))
    }

    private drainHistoryOperations(timeoutMs: number): Promise<BackgroundQueueFlushResult> {
        const before = this.historySettled
        return new Promise((resolve) => {
            const finish = (): void => {
                clearTimeout(timer)
                this.historyDrainWaiters.delete(finish)
                resolve({
                    flushed: this.historySettled - before,
                    remaining: this.historyOutstanding
                })
            }
            const timer = setTimeout(finish, Math.max(0, timeoutMs))
            this.historyDrainWaiters.add(finish)
            if (this.historyOperations.size === 0) finish()
        })
    }

    public constructor(
        stores: WriteBehindStores,
        logger: Logger,
        options: WaWriteBehindOptions = {}
    ) {
        this.logger = logger
        this.stores = stores
        this.options = options
        this.flushTimeoutMs = options.flushTimeoutMs ?? 5_000
        this.queues = this.buildQueues()
        this.destroyed = false
    }

    private buildQueues(): typeof this.queues {
        const stores = this.stores
        const options = this.options
        const queueOptions = (domain: string) => ({
            maxPendingKeys: options.maxPendingKeys ?? 4_096,
            maxWriteConcurrency: options.maxWriteConcurrency ?? 4,
            flushTimeoutMs: this.flushTimeoutMs,
            onError: (key: string, error: unknown, attempt: number) => {
                this.logger.warn('write-behind error', {
                    domain,
                    key,
                    attempt,
                    message: toError(error).message
                })
            },
            onPressure: (pendingKeys: number) => {
                this.logger.warn('write-behind pressure', {
                    domain,
                    pendingKeys
                })
            },
            onDiscard: (key: string) => {
                this.logger.warn('write-behind discarded pending write', {
                    domain,
                    key
                })
            }
        })
        return {
            messages: new BackgroundQueue((_key, value) => stores.messageStore.upsert(value), {
                ...queueOptions('messages'),
                batchWriter: (entries) =>
                    stores.messageStore.upsertBatch(entries.map((e) => e.value))
            }),
            threads: new BackgroundQueue((_key, value) => stores.threadStore.upsert(value), {
                ...queueOptions('threads'),
                coalesce: (previous, incoming) => mergeThread(previous, incoming),
                batchWriter: (entries) =>
                    stores.threadStore.upsertBatch(entries.map((e) => e.value))
            }),
            contacts: new BackgroundQueue((_key, value) => stores.contactStore.upsert(value), {
                ...queueOptions('contacts'),
                coalesce: (previous, incoming) => mergeContact(previous, incoming),
                batchWriter: (entries) =>
                    stores.contactStore.upsertBatch(entries.map((e) => e.value))
            })
        }
    }

    /**
     * Re-arm the queues after a previous {@link destroy} call. Idempotent —
     * a no-op when the persistence is already live. Used by `WaClient.connect`
     * to recover from a logout/clear-stored-state cycle without leaking the
     * old destroyed queues into the next session.
     */
    public restart(): void {
        if (!this.destroyed) {
            return
        }
        this.queues = this.buildQueues()
        this.destroyed = false
    }

    public persistMessage(record: WaStoredMessageRecord): void {
        this.queues.messages.enqueue(`msg:${record.id}`, record)
    }

    public persistMessageAsync(record: WaStoredMessageRecord): Promise<void> {
        return this.queues.messages.enqueueAsync(`msg:${record.id}`, record)
    }

    public persistThread(record: WaStoredThreadRecord): void {
        this.queues.threads.enqueue(`thread:${record.jid}`, record)
    }

    public persistThreadAsync(record: WaStoredThreadRecord): Promise<void> {
        return this.queues.threads.enqueueAsync(`thread:${record.jid}`, record)
    }

    public persistContact(record: WaStoredContactRecord): void {
        this.queues.contacts.enqueue(`contact:${record.jid}`, record)
    }

    public persistContactAsync(record: WaStoredContactRecord): Promise<void> {
        return this.queues.contacts.enqueueAsync(`contact:${record.jid}`, record)
    }

    public async flush(timeoutMs: number = this.flushTimeoutMs): Promise<WriteBehindDrainResult> {
        const [messages, threads, contacts, history] = await Promise.all([
            this.queues.messages.flush(timeoutMs),
            this.queues.threads.flush(timeoutMs),
            this.queues.contacts.flush(timeoutMs),
            this.drainHistoryOperations(timeoutMs)
        ])
        const result = this.toDrainResult(messages, threads, contacts, history)
        if (result.remaining > 0) {
            this.logger.warn('write-behind flush finished with pending writes', {
                messagesRemaining: messages.remaining,
                threadsRemaining: threads.remaining,
                contactsRemaining: contacts.remaining,
                historyRemaining: result.history.remaining
            })
        }
        return result
    }

    public async destroy(timeoutMs: number = this.flushTimeoutMs): Promise<WriteBehindDrainResult> {
        this.destroyed = true
        const [messages, threads, contacts, history] = await Promise.all([
            this.queues.messages.destroy(timeoutMs),
            this.queues.threads.destroy(timeoutMs),
            this.queues.contacts.destroy(timeoutMs),
            this.drainHistoryOperations(timeoutMs)
        ])
        this.destroyed = true
        const result = this.toDrainResult(messages, threads, contacts, history)
        if (result.remaining > 0) {
            this.logger.warn('write-behind destroy finished with pending writes', {
                messagesRemaining: messages.remaining,
                threadsRemaining: threads.remaining,
                contactsRemaining: contacts.remaining,
                historyRemaining: result.history.remaining
            })
        }
        return result
    }

    private toDrainResult(
        messages: BackgroundQueueFlushResult,
        threads: BackgroundQueueFlushResult,
        contacts: BackgroundQueueFlushResult,
        history: BackgroundQueueFlushResult
    ): WriteBehindDrainResult {
        // A flush does not fence new registrations. Recheck actual ownership at
        // return, not the possibly stale snapshot from the parallel drain start.
        history = { ...history, remaining: this.historyOutstanding }
        return {
            messages,
            threads,
            contacts,
            history,
            flushed: messages.flushed + threads.flushed + contacts.flushed,
            // Queue entries and owned promise lifetimes are separate accounting
            // units and may overlap. This conservative drain indicator is not
            // a count of unique provider writes; zero requires both to settle.
            remaining:
                messages.remaining + threads.remaining + contacts.remaining + history.remaining
        }
    }
}
