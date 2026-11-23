'use strict';

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const { EventEmitter, once } = require('node:events');
const fs = require('node:fs');
const https = require('node:https');
const { join } = require('node:path');
const { tmpdir } = require('node:os');
const test = require('node:test');

const certificateDirectory = process.env.KAFKA_HTTP_TEST_CERT_DIR;

if (!certificateDirectory) {
    test('HTTPS gateway integration', () => {
        const directory = fs.mkdtempSync(join(tmpdir(), 'kafka-http-test-'));
        try {
            const key = join(directory, 'key.pem');
            const cert = join(directory, 'cert.pem');
            const openssl = process.env.KAFKA_HTTP_TEST_OPENSSL || 'openssl';
            const generated = spawnSync(openssl, [
                'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                '-subj', '/CN=gateway.example.test',
                '-addext', 'subjectAltName=DNS:gateway.example.test',
                '-keyout', key, '-out', cert,
            ], { encoding: 'utf8' });
            assert.ifError(generated.error);
            assert.equal(generated.status, 0, generated.stderr);
            // The certificate is trusted only through the gateway's CA certificate setting.
            const env = { ...process.env, KAFKA_HTTP_TEST_CERT_DIR: directory };
            delete env.NODE_TEST_CONTEXT;
            delete env.NODE_EXTRA_CA_CERTS;
            const result = spawnSync(process.execPath, ['--test', __filename], {
                env, encoding: 'utf8', timeout: 15000,
            });
            assert.ifError(result.error);
            assert.equal(result.status, 0, result.stdout + result.stderr);
        } finally {
            fs.rmSync(join(directory, 'key.pem'), { force: true });
            fs.rmSync(join(directory, 'cert.pem'), { force: true });
            fs.rmdirSync(directory);
        }
    });
} else {
    test('publishes, polls and verifies the gateway hostname', { timeout: 14000 }, async () => {
        const requests = [];
        let publishing = 0;
        let maxPublishing = 0;
        const server = https.createServer({
            key: fs.readFileSync(join(certificateDirectory, 'key.pem')),
            cert: fs.readFileSync(join(certificateDirectory, 'cert.pem')),
        }, async (request, response) => {
            const url = new URL(request.url, 'https://gateway.example.test');
            const chunks = [];
            for await (const chunk of request) chunks.push(chunk);
            requests.push({
                method: request.method, url, host: request.headers.host,
                authorization: request.headers.authorization,
                body: Buffer.concat(chunks).toString('utf8'),
            });
            const topic = 'device-one';
            response.setHeader('Content-Type', 'application/json');
            response.on('error', () => {});
            if (url.pathname.endsWith('/topics/backlog/messages')) {
                // Ten records of about 210 KiB each; endOffset is exclusive.
                const start = Number(url.searchParams.get('offset') ?? 0);
                const end = Math.min(start + Number(url.searchParams.get('limit')), 10);
                const records = [];
                for (let index = start; index < end; index += 1) {
                    records.push({ key: 'backlog', offset: String(index), timestamp: '1', value: String(index).padEnd(210 * 1024, '.') });
                }
                response.end(JSON.stringify({
                    topic: 'backlog', partition: 0, endOffset: '10', nextOffset: String(end), records,
                }));
                return;
            }
            if (request.method === 'POST') {
                publishing += 1;
                maxPublishing = Math.max(maxPublishing, publishing);
                if (JSON.parse(requests.at(-1).body).value?.delay) await new Promise(resolve => setTimeout(resolve, 150));
                publishing -= 1;
                response.statusCode = 201;
                response.end(JSON.stringify({ topic, key: topic, partition: 0, offset: '1' }));
                return;
            }
            const cursor = url.searchParams.get('offset');
            const latest = cursor === null;
            response.end(JSON.stringify({
                topic, partition: 0, endOffset: latest ? '1' : '2', nextOffset: latest ? '1' : '2',
                records: cursor === '2' ? [] : [
                    { key: topic, offset: latest ? '0' : '1', timestamp: '1234', value: { rpm: latest ? 3 : 5 } },
                ],
            }));
        });
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;
        const types = new Map();
        const nodes = new Map();
        require('../nodes/kafka-http-gateway')({
            nodes: {
                registerType: (role, handler) => types.set(role, handler),
                getNode: id => nodes.get(id),
                createNode: (instance, config) => {
                    const events = new EventEmitter();
                    const values = new Map();
                    // Asynchronous store: only the callback form of get and set is supported.
                    const context = {
                        get: (key, callback) => setImmediate(() => callback(null, values.get(key))),
                        set: (key, value, callback) => setImmediate(() => { values.set(key, value); callback(null); }),
                    };
                    Object.assign(instance, {
                        on: events.on.bind(events),
                        once: events.once.bind(events),
                        emit: events.emit.bind(events),
                        removeListener: events.removeListener.bind(events),
                        status: () => {},
                        error: message => { throw new Error(message); },
                        context: () => context,
                        credentials: config.credentials,
                        send: output => events.emit('sent', output),
                    });
                },
            },
        });
        const deploy = (role, config) => new (types.get(role))(config);
        const config = {
            baseUrl: 'https://gateway.example.test:' + port, connectAddress: '127.0.0.1',
            ca: fs.readFileSync(join(certificateDirectory, 'cert.pem'), 'utf8'),
            credentials: { username: 'sample', password: 'test-only' },
        };
        const gateway = deploy('kafka-http-gateway-config', config);
        nodes.set('gateway', gateway);
        const producer = deploy('kafka-http-producer', { gateway: 'gateway', topic: 'device-one' });
        let consumer;
        let invalidGateway;
        let untrustedGateway;
        let smallGateway;
        let backlogConsumer;
        try {
            const publish = (msg, node = producer) => new Promise((resolve, reject) => {
                let output;
                node.emit('input', msg, value => { output = value; }, error => {
                    if (error) reject(error);
                    else resolve(output);
                });
            });
            const published = await publish({ payload: { rpm: 5 } });
            assert.deepEqual(published.kafka, { topic: 'device-one', key: 'device-one', partition: 0, offset: '1' });
            assert.deepEqual(JSON.parse(requests[0].body), { value: { rpm: 5 } });
            assert.equal(requests[0].host, 'gateway.example.test:' + port);
            assert.equal(requests[0].authorization, 'Basic ' + Buffer.from('sample:test-only').toString('base64'));
            assert.equal(requests[0].url.pathname, '/kafka/v1/topics/device-one/messages');
            await assert.rejects(publish({}), /msg.payload/);
            const raw = await publish({ payload: Buffer.from([0, 1, 254, 255]) });
            assert.equal(raw.kafka.offset, '1');
            assert.deepEqual(JSON.parse(requests[1].body), { valueBase64: 'AAH+/w==' });
            await publish({ payload: new Uint8Array([1, 2, 3, 4]).subarray(1, 3) });
            assert.deepEqual(JSON.parse(requests[2].body), { valueBase64: 'AgM=' });
            requests.splice(1, 2);

            consumer = deploy('kafka-http-consumer', { gateway: 'gateway', topic: 'device-one', interval: 1000 });
            const [record] = await once(consumer, 'sent', { signal: AbortSignal.timeout(5000) });
            assert.deepEqual(record.payload, { rpm: 5 });
            assert.equal(record.kafka.offset, '1');
            assert.equal(requests[1].url.searchParams.get('limit'), '1');
            assert.equal(requests[2].url.searchParams.get('limit'), '100');
            assert.equal(requests[2].url.searchParams.get('offset'), '1');
            assert.equal(requests.length, 3);
            consumer.emit('close');

            invalidGateway = deploy('kafka-http-gateway-config', {
                ...config, baseUrl: 'https://other.example.test:' + port,
            });
            await assert.rejects(invalidGateway.client.request('GET', 'device-one'), { code: 'ERR_TLS_CERT_ALTNAME_INVALID' });
            assert.equal(requests.length, 3);

            const ordered = await Promise.all([
                publish({ payload: { n: 1, delay: true } }),
                publish({ payload: { n: 2 } }),
                publish({ payload: { n: 3 } }),
            ]);
            assert.equal(ordered.length, 3);
            assert.deepEqual(requests.slice(3).map(entry => JSON.parse(entry.body).value.n), [1, 2, 3]);
            assert.equal(maxPublishing, 1, 'publishes must not overlap');

            // Without the CA certificate setting, the self-signed certificate is rejected.
            untrustedGateway = deploy('kafka-http-gateway-config', { ...config, ca: '' });
            await assert.rejects(untrustedGateway.client.request('GET', 'device-one'), { code: /SELF_SIGNED/ });
            assert.throws(() => deploy('kafka-http-gateway-config', { ...config, ca: 'not a certificate' }), /PEM/);
            assert.equal(requests.length, 6);

            // An empty producer topic uses msg.topic.
            const dynamic = deploy('kafka-http-producer', { gateway: 'gateway', topic: '' });
            await publish({ topic: 'device-one', payload: 1 }, dynamic);
            assert.equal(requests[6].url.pathname, '/kafka/v1/topics/device-one/messages');
            await assert.rejects(publish({ topic: 'Bad Topic', payload: 1 }, dynamic), /gateway topic/);
            await assert.rejects(publish({ payload: 1 }, dynamic), /gateway topic/);
            assert.equal(requests.length, 7);

            // A 1 MiB response limit forces smaller pages; full pages are read without waiting.
            smallGateway = deploy('kafka-http-gateway-config', { ...config, maxResponseMiB: 1, maxRequestKiB: 1 });
            nodes.set('small', smallGateway);
            await assert.rejects(publish({ payload: 'x'.repeat(2048) }, deploy('kafka-http-producer', { gateway: 'small', topic: 'device-one' })), /1 KiB request limit/);
            const received = [];
            backlogConsumer = deploy('kafka-http-consumer', {
                gateway: 'small', topic: 'backlog', start: 'earliest', limit: 10, interval: 60000,
            });
            backlogConsumer.on('sent', output => received.push(output.kafka.offset));
            const deadline = Date.now() + 5000;
            while (received.length < 10 && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
            assert.deepEqual(received, ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']);
            const backlog = requests.slice(7).filter(entry => entry.url.pathname.includes('/backlog/'));
            assert.deepEqual(backlog.map(entry => entry.url.searchParams.get('limit')), ['10', '5', '3', '3', '3', '3']);
            assert.deepEqual(backlog.map(entry => entry.url.searchParams.get('offset')), [null, null, null, '3', '6', '9']);
        } finally {
            backlogConsumer?.emit('close');
            smallGateway?.emit('close');
            untrustedGateway?.emit('close');
            consumer?.emit('close');
            invalidGateway?.emit('close');
            gateway.emit('close');
            await new Promise(resolve => server.close(resolve));
        }
    });
}
