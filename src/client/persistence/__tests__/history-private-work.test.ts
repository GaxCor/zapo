import assert from 'node:assert/strict'
import test from 'node:test'

import { WriteBehindPersistence } from '@client/persistence/WriteBehindPersistence'
import { createNoopLogger } from '@infra/log/types'

function owner(): WriteBehindPersistence {
    const store = { upsert: async () => undefined, upsertBatch: async () => undefined }
    return new WriteBehindPersistence(
        {
            messageStore: store as never,
            threadStore: store as never,
            contactStore: store as never
        },
        createNoopLogger()
    )
}

function deferred() {
    let resolve!: () => void
    let reject!: (error: Error) => void
    const promise = new Promise<void>((yes, no) => {
        resolve = yes
        reject = no
    })
    return { promise, resolve, reject }
}

for (const outcome of ['resolve', 'reject'] as const) {
    test(`private history ownership remains through finite destroy and late ${outcome}`, async () => {
        const persistence = owner()
        const pending = deferred()
        assert.equal(
            persistence.ownHistoryOperation(() => pending.promise),
            pending.promise
        )
        const during = await persistence.flush(10)
        assert.equal(during.history.remaining, 1)
        assert.equal(during.remaining, 1)
        const shutdown = await persistence.destroy(10)
        assert.equal(shutdown.history.remaining, 1)
        assert.equal(shutdown.remaining, 1)
        if (outcome === 'resolve') pending.resolve()
        else pending.reject(new Error('synthetic private value must not be logged'))
        await new Promise<void>((done) => setImmediate(done))
        const after = await persistence.flush(10)
        assert.equal(after.history.remaining, 0)
        assert.equal(after.remaining, 0)
    })
}

test('history ownership observes rejection without changing the original promise', async () => {
    const persistence = owner()
    const failure = new Error('synthetic failure')
    const promise = Promise.reject(failure)
    assert.equal(
        persistence.ownHistoryOperation(() => promise),
        promise
    )
    await assert.rejects(promise, (error) => error === failure)
    assert.equal((await persistence.flush(10)).history.remaining, 0)
    await persistence.destroy(10)
})

test('normal history private completion removes ownership once without serialization', async () => {
    const persistence = owner()
    const one = deferred()
    const two = deferred()
    const first = persistence.ownHistoryOperation(() => one.promise)
    const second = persistence.ownHistoryOperation(() => two.promise)
    const drain = persistence.flush(1_000)
    two.resolve()
    await second
    assert.equal((await persistence.flush(0)).history.remaining, 1)
    one.resolve()
    await first
    assert.deepEqual((await drain).history, { flushed: 2, remaining: 0 })
    await persistence.destroy()
})

test('repeated failed-job ownership stays bounded after eventual settlement and restart', async () => {
    const persistence = owner()
    for (let index = 0; index < 100; index += 1) {
        const pending = deferred()
        persistence.ownHistoryOperation(() => pending.promise)
        if (index % 2) pending.reject(new Error('synthetic'))
        else pending.resolve()
    }
    assert.equal((await persistence.flush(1_000)).history.remaining, 0)
    await persistence.destroy()
    assert.throws(() => persistence.ownHistoryOperation(() => Promise.resolve()), /admission/)
    persistence.restart()
    await persistence.ownHistoryOperation(() => Promise.resolve())
    assert.equal((await persistence.destroy()).remaining, 0)
})

test('history private limit rejects before starting any unowned provider operation', async () => {
    const persistence = owner()
    const pending = Array.from({ length: 4_096 }, () => deferred())
    for (const item of pending) persistence.ownHistoryOperation(() => item.promise)
    let starts = 0
    assert.throws(
        () =>
            persistence.ownHistoryOperation(() => {
                starts += 1
                return Promise.resolve()
            }),
        /admission/
    )
    assert.equal(starts, 0)
    assert.equal((await persistence.flush(0)).history.remaining, 4_096)
    for (const item of pending) item.resolve()
    assert.equal((await persistence.destroy(1_000)).remaining, 0)
})

test('flush rechecks ownership registered while another private queue drains', async () => {
    const queue = deferred()
    const pending = deferred()
    const store = { upsert: () => queue.promise, upsertBatch: () => queue.promise }
    const persistence = new WriteBehindPersistence(
        { messageStore: store as never, threadStore: store as never, contactStore: store as never },
        createNoopLogger()
    )
    persistence.persistMessage({ id: 'synthetic' } as never)
    const flushing = persistence.flush(1_000)
    await new Promise<void>((done) => setImmediate(done))
    persistence.ownHistoryOperation(() => pending.promise)
    queue.resolve()
    assert.equal((await flushing).history.remaining, 1)
    pending.resolve()
    assert.equal((await persistence.destroy()).remaining, 0)
})

test('shared provider promise cannot bypass operation bounds or accumulate tracker observers', async () => {
    const persistence = owner()
    const shared = deferred()
    for (let index = 0; index < 4_096; index += 1)
        persistence.ownHistoryOperation(() => shared.promise)
    assert.equal((await persistence.flush(0)).history.remaining, 4_096)
    assert.throws(() => persistence.ownHistoryOperation(() => shared.promise), /admission/)
    shared.resolve()
    assert.equal((await persistence.destroy()).history.remaining, 0)
})
