import assert from 'node:assert/strict'
import test from 'node:test'

import { createNoopLogger } from '@infra/log/types'
import { WaNodeTransport } from '@transport/node/WaNodeTransport'
import type { BinaryNode } from '@transport/types'

test('captured sender is generation-bound while normal sends use current comms', async () => {
    let generation = 7
    const sent: Uint8Array[] = []
    const comms = {
        getConnectionGeneration: () => generation,
        sendFrame: async (frame: Uint8Array) => {
            sent.push(frame)
        },
        sendFrameForGeneration: async (frame: Uint8Array, expected: number) => {
            if (expected !== generation) {
                throw new Error('captured connection generation is stale')
            }
            sent.push(frame)
        }
    }
    const transport = new WaNodeTransport(createNoopLogger())
    transport.bindComms(comms as never)
    const boundSender = transport.captureBoundSender()
    const receipt: BinaryNode = { tag: 'receipt', attrs: { id: 'r1', type: 'delivery' } }

    await boundSender(receipt)
    assert.equal(sent.length, 1, 'same-generation receipt should be sent')

    generation += 1
    await assert.rejects(boundSender(receipt), /generation is stale/)
    assert.equal(sent.length, 1, 'old receipt must not be sent through a replacement generation')

    await transport.sendNode(receipt)
    assert.equal(sent.length, 2, 'ordinary sends retain current-connection behavior')
})
