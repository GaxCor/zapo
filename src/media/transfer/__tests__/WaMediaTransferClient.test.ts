import assert from 'node:assert/strict'
import { createServer, type Server } from 'node:http'
import test from 'node:test'

import { WaMediaTransferClient } from '@media/transfer/WaMediaTransferClient'

function deferred(): { readonly promise: Promise<void>; readonly resolve: () => void } {
    let resolve!: () => void
    const promise = new Promise<void>((done) => {
        resolve = done
    })
    return { promise, resolve }
}

async function listen(server: Server): Promise<string> {
    await new Promise<void>((resolve, reject) => {
        server.once('error', reject)
        server.listen(0, '127.0.0.1', resolve)
    })
    const address = server.address()
    assert.ok(address && typeof address !== 'string')
    return `http://127.0.0.1:${address.port}/history`
}

async function close(server: Server): Promise<void> {
    if (!server.listening) return
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()))
    })
}

test('media transfer abort before headers terminates the HTTP request', async () => {
    const requestReceived = deferred()
    const responseClosed = deferred()
    const server = createServer((_request, response) => {
        requestReceived.resolve()
        response.once('close', responseClosed.resolve)
    })
    const url = await listen(server)
    const controller = new AbortController()
    const transfer = new WaMediaTransferClient().downloadStream({
        url,
        signal: controller.signal,
        timeoutMs: 5_000
    })
    const rejected = assert.rejects(transfer, /abort/i)

    try {
        await requestReceived.promise
        controller.abort(new Error('test abort before headers'))
        await rejected
        await responseClosed.promise
    } finally {
        await close(server)
    }
})

test('media transfer abort after headers destroys a stalled response body', async () => {
    const headersSent = deferred()
    const responseClosed = deferred()
    const server = createServer((_request, response) => {
        response.once('close', responseClosed.resolve)
        response.writeHead(200)
        response.flushHeaders()
        headersSent.resolve()
    })
    const url = await listen(server)
    const controller = new AbortController()

    try {
        const transfer = await new WaMediaTransferClient().downloadStream({
            url,
            signal: controller.signal,
            timeoutMs: 5_000
        })
        await headersSent.promise
        const body = transfer.body
        assert.ok(body, 'the successful response must expose its body')
        const bodyClosed = new Promise<void>((resolve) => body.once('close', resolve))

        controller.abort(new Error('test abort after headers'))

        await Promise.all([bodyClosed, responseClosed.promise])
        assert.equal(body.destroyed, true)
    } finally {
        await close(server)
    }
})

test('media transfer keeps successful body consumption compatible', async () => {
    const server = createServer((_request, response) => {
        response.writeHead(200)
        response.end('ok')
    })
    const url = await listen(server)

    try {
        const transfer = await new WaMediaTransferClient().downloadStream({
            url,
            timeoutMs: 5_000
        })
        assert.ok(transfer.body)
        const chunks: Buffer[] = []
        for await (const chunk of transfer.body) {
            chunks.push(Buffer.from(chunk))
        }
        assert.equal(Buffer.concat(chunks).toString(), 'ok')
    } finally {
        await close(server)
    }
})
