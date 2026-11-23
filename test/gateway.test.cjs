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
            const env = { ...process.env, KAFKA_HTTP_TEST_CERT_DIR: directory, NODE_EXTRA_CA_CERTS: cert };
            delete env.NODE_TEST_CONTEXT;
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
    test('publishes, polls and verifies the gateway hostname', { timeout: 12000 }, async () => {
        const requests = [];
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
            if (request.method === 'POST') {
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
                    const context = new Map();
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
            credentials: { username: 'sample', password: 'test-only' },
        };
        const gateway = deploy('kafka-http-gateway-config', config);
        nodes.set('gateway', gateway);
        const producer = deploy('kafka-http-producer', { gateway: 'gateway', topic: 'device-one' });
        let consumer;
        let invalidGateway;
        try {
            const publish = msg => new Promise((resolve, reject) => {
                let output;
                producer.emit('input', msg, value => { output = value; }, error => {
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
        } finally {
            consumer?.emit('close');
            invalidGateway?.emit('close');
            gateway.emit('close');
            await new Promise(resolve => server.close(resolve));
        }
    });
}
