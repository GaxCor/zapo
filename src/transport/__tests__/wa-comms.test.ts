import assert from 'node:assert/strict'
import test from 'node:test'

import { createNoopLogger } from '@infra/log/types'
import type { RawWebSocketConstructor } from '@transport/types'
import { WaComms } from '@transport/WaComms'

const NOOP_SOCKET_CTOR = class {
    public binaryType = 'arraybuffer'
    public readyState = 0
    public onopen: unknown = null
    public onclose: unknown = null
    public onerror: unknown = null
    public onmessage: unknown = null
    public close(): void {
        return
    }
    public send(): void {
        return
    }
} as unknown as RawWebSocketConstructor

function createComms(): WaComms {
    return new WaComms(
        {
            urls: ['wss://example.invalid/ws'],
            rawWebSocketConstructor: NOOP_SOCKET_CTOR,
            noise: {
                clientStaticKeyPair: {
                    pubKey: new Uint8Array(32),
                    privKey: new Uint8Array(32)
                },
                isRegistered: false
            }
        },
        createNoopLogger()
    )
}

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
    let resolve!: () => void
    const promise = new Promise<void>((done) => {
        resolve = done
    })
    return { promise, resolve }
}

test('closeSocketAndResume does not revive a comms that was never started', async () => {
    const comms = createComms()
    assert.equal(comms.getCommsState().started, false)

    await comms.closeSocketAndResume()

    assert.equal(comms.getCommsState().started, false)
    assert.equal(comms.getCommsState().connected, false)
})

test('closeSocketAndResume does not revive a comms after stopComms', async () => {
    const comms = createComms()
    await comms.stopComms()

    await comms.closeSocketAndResume()

    assert.equal(comms.getCommsState().started, false)
    assert.equal(comms.getCommsState().connected, false)
})

test('generation-bound frame send rejects a receipt encrypted across session replacement', async () => {
    const comms = createComms()
    const internals = comms as unknown as {
        socket: { send: (wire: Uint8Array) => Promise<void> }
        replaceNoiseSession(session: unknown): void
    }
    const sent: Uint8Array[] = []
    internals.socket.send = async (wire) => {
        sent.push(wire)
    }
    internals.replaceNoiseSession({
        encryptFrame: async () => new Uint8Array([1])
    })
    const sameGeneration = comms.getConnectionGeneration()
    await comms.sendFrameForGeneration(new Uint8Array([10]), sameGeneration)
    assert.deepEqual(sent, [new Uint8Array([1])])

    const encryptionStarted = deferred()
    const finishEncryption = deferred()
    internals.replaceNoiseSession({
        encryptFrame: async () => {
            encryptionStarted.resolve()
            await finishEncryption.promise
            return new Uint8Array([2])
        }
    })
    const capturedGeneration = comms.getConnectionGeneration()
    const lateSend = comms.sendFrameForGeneration(new Uint8Array([11]), capturedGeneration)
    await encryptionStarted.promise

    // A replacement Noise session is installed while encryption of the old
    // receipt is suspended; the post-encryption generation check must stop it.
    internals.replaceNoiseSession({
        encryptFrame: async () => new Uint8Array([3])
    })
    finishEncryption.resolve()

    await assert.rejects(lateSend, /generation is stale/)
    await assert.rejects(
        comms.sendFrameForGeneration(new Uint8Array([12]), capturedGeneration),
        /generation is stale/
    )
    assert.deepEqual(sent, [new Uint8Array([1])])
})
