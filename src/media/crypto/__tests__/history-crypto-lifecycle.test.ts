import assert from 'node:assert/strict'
import { Readable } from 'node:stream'
import test from 'node:test'
import { setTimeout as delay } from 'node:timers/promises'

import { WaMediaCrypto } from '@media/crypto/WaMediaCrypto'

async function within<T>(promise: Promise<T>): Promise<T> {
    let timer!: NodeJS.Timeout
    try {
        return await Promise.race([
            promise,
            new Promise<never>((_yes, no) => {
                timer = setTimeout(() => no(new Error('synthetic crypto ceiling')), 1_000)
            })
        ])
    } finally {
        clearTimeout(timer)
    }
}

for (const mode of [
    'before-write',
    'close',
    'error',
    'close-error-drain',
    'drain-close',
    'abort'
] as const) {
    test(`history crypto downstream ${mode} settles metadata once and removes pump listeners`, async () => {
        const key = new Uint8Array(32)
        const ciphertext = await WaMediaCrypto.encryptBytes(
            'history',
            key,
            new Uint8Array(65_536).fill(9),
            { sidecar: false }
        )
        const source = Readable.from([ciphertext.ciphertextHmac])
        const decrypted = await WaMediaCrypto.decryptReadable(source, {
            mediaType: 'history',
            mediaKey: key
        })
        let settlements = 0
        const metadata = decrypted.metadata.then(
            () => {
                settlements += 1
            },
            () => {
                settlements += 1
            }
        )
        if (mode !== 'before-write') {
            await within(
                (async () => {
                    while (decrypted.plaintext.listenerCount('drain') === 0) await delay(1)
                })()
            )
        }
        if (mode === 'error') decrypted.plaintext.destroy(new Error('synthetic downstream'))
        else if (mode === 'close-error-drain') {
            decrypted.plaintext.destroy()
            decrypted.plaintext.emit('close')
            decrypted.plaintext.emit('error', new Error('synthetic race'))
            decrypted.plaintext.emit('drain')
        } else if (mode === 'drain-close') {
            decrypted.plaintext.emit('drain')
            decrypted.plaintext.destroy()
        } else if (mode === 'abort') {
            const controller = new AbortController()
            controller.signal.addEventListener('abort', () => decrypted.plaintext.destroy(), {
                once: true
            })
            controller.abort()
        } else decrypted.plaintext.destroy()
        await within(metadata)
        await new Promise<void>((done) => setImmediate(done))
        assert.equal(settlements, 1)
        assert.equal(source.destroyed, true)
        assert.equal(decrypted.plaintext.destroyed, true)
        assert.equal(decrypted.plaintext.listenerCount('drain'), 0)
        assert.equal(decrypted.plaintext.listenerCount('error'), 0)
        assert.equal(decrypted.plaintext.listenerCount('close'), 0)
    })
}

test('natural successful history crypto keeps hashes/bytes exact and removes owned listeners', async () => {
    const key = new Uint8Array(32)
    const expected = new Uint8Array(65_536).fill(7)
    const ciphertext = await WaMediaCrypto.encryptBytes('history', key, expected, {
        sidecar: false
    })
    const source = Readable.from([ciphertext.ciphertextHmac])
    const decrypted = await WaMediaCrypto.decryptReadable(source, {
        mediaType: 'history',
        mediaKey: key,
        expectedFileSha256: ciphertext.fileSha256,
        expectedFileEncSha256: ciphertext.fileEncSha256
    })
    const chunks: Uint8Array[] = []
    for await (const chunk of decrypted.plaintext) chunks.push(chunk as Uint8Array)
    const metadata = await within(decrypted.metadata)
    assert.deepEqual(metadata.fileSha256, ciphertext.fileSha256)
    assert.deepEqual(metadata.fileEncSha256, ciphertext.fileEncSha256)
    assert.equal(
        chunks.reduce((total, chunk) => total + chunk.length, 0),
        expected.length
    )
    let offset = 0
    for (const chunk of chunks) {
        assert.deepEqual(new Uint8Array(chunk), expected.subarray(offset, offset + chunk.length))
        offset += chunk.length
    }
    assert.equal(decrypted.plaintext.listenerCount('drain'), 0)
    // Async-iterator internals may retain their own listeners on completed streams.
    assert.ok(decrypted.plaintext.listenerCount('close') <= 1)
})
