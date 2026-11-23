'use strict';

const https = require('node:https');
const tls = require('node:tls');
const { isIP } = require('node:net');
const { createHash } = require('node:crypto');

const responseTooLarge = 'ERR_KAFKA_HTTP_RESPONSE_TOO_LARGE';

function offset(value) {
    return typeof value === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(value)
        && BigInt(value) <= 9223372036854775807n;
}

function integer(value, min, max, label) {
    const number = Number(value);
    if (!Number.isSafeInteger(number) || number < min || number > max) {
        throw new Error(label + ' must be between ' + min + ' and ' + max);
    }
    return number;
}

// Buffers, typed arrays and ArrayBuffers are sent as raw bytes; anything else is a JSON value.
function binary(value) {
    if (Buffer.isBuffer(value)) return value;
    if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
    if (value instanceof ArrayBuffer) return Buffer.from(value);
    return undefined;
}

// A custom CA is trusted in addition to the system roots, like NODE_EXTRA_CA_CERTS.
function certificateAuthorities(value) {
    const pem = String(value || '').trim();
    if (!pem) return undefined;
    if (!pem.includes('-----BEGIN CERTIFICATE-----')) {
        throw new Error('CA certificate must be in PEM format');
    }
    try {
        tls.createSecureContext({ ca: pem });
    } catch {
        throw new Error('CA certificate could not be parsed');
    }
    return [...tls.rootCertificates, pem];
}

function topicName(value) {
    if (typeof value !== 'string' || !/^[a-z0-9-]{1,249}$/.test(value)) {
        throw new Error('Enter a gateway topic containing lowercase letters, digits or hyphens');
    }
    return value;
}

function gatewayOptions(config, credentials = {}) {
    const base = new URL(config.baseUrl);
    if (base.protocol !== 'https:' || base.username || base.password || base.search || base.hash) {
        throw new Error('Gateway URL must use HTTPS without credentials, query or fragment');
    }
    const address = String(config.connectAddress || '').trim();
    const hostname = base.hostname.replace(/^\[|\]$/g, '');
    if (address && (!isIP(address) || isIP(hostname))) {
        throw new Error('A connection address requires an IP override and a DNS name in the gateway URL');
    }
    const { username, password } = credentials;
    if (Boolean(username) !== Boolean(password) || username?.includes(':')) {
        throw new Error('Supply a username without colons and a password, or leave both empty');
    }
    const authorization = username
        ? 'Basic ' + Buffer.from(username + ':' + password, 'utf8').toString('base64')
        : undefined;
    return {
        base, hostname, address, authorization,
        ca: certificateAuthorities(config.ca),
        timeout: integer(config.timeout || 20000, 500, 120000, 'Timeout'),
        maxRequestKiB: integer(config.maxRequestKiB || 128, 1, 16384, 'Maximum request size'),
        maxResponseMiB: integer(config.maxResponseMiB || 16, 1, 256, 'Maximum response size'),
    };
}

function createClient(config, credentials) {
    const { base, hostname, address, authorization, ca, timeout, maxRequestKiB, maxResponseMiB } = gatewayOptions(config, credentials);
    const maxRequestBytes = maxRequestKiB * 1024;
    const maxResponseBytes = maxResponseMiB * 1024 * 1024;
    const agent = new https.Agent({ keepAlive: true, maxSockets: 8, maxFreeSockets: 2 });

    async function request(method, topic, { query, value } = {}) {
        const path = base.pathname.replace(/\/+$/, '') + '/kafka/v1/topics/' + topicName(topic) + '/messages';
        const search = query ? '?' + new URLSearchParams(query).toString() : '';
        const headers = { Host: base.host, Accept: 'application/json' };
        if (authorization) headers.Authorization = authorization;
        let body;
        if (method === 'POST') {
            if (value === undefined) throw new Error('msg.payload must be a JSON value or a Buffer');
            const bytes = binary(value);
            body = JSON.stringify(bytes ? { valueBase64: bytes.toString('base64') } : { value });
            if (body === '{}') throw new Error('msg.payload must be a JSON value or a Buffer');
            const size = Buffer.byteLength(body);
            if (size > maxRequestBytes) {
                throw new Error('Payload exceeds the ' + maxRequestKiB + ' KiB request limit of the gateway configuration');
            }
            headers['Content-Type'] = 'application/json';
            headers['Content-Length'] = size;
        }
        return new Promise((resolve, reject) => {
            const req = https.request({
                hostname: address || hostname,
                servername: isIP(hostname) ? undefined : hostname,
                port: base.port || 443,
                path: path + search,
                method,
                headers,
                agent,
                ca,
                rejectUnauthorized: true,
                signal: AbortSignal.timeout(timeout),
            }, response => {
                const chunks = [];
                let bytes = 0;
                response.on('error', reject);
                response.on('data', chunk => {
                    bytes += chunk.length;
                    if (bytes > maxResponseBytes) {
                        const error = new Error('Gateway response exceeds the ' + maxResponseMiB + ' MiB response limit');
                        error.code = responseTooLarge;
                        response.destroy(error);
                        return;
                    }
                    chunks.push(chunk);
                });
                response.on('end', () => {
                    let json;
                    try { json = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { /* checked by the caller */ }
                    resolve({ status: response.statusCode, body: json });
                });
            });
            req.on('error', reject);
            req.end(body);
        });
    }

    return { request, identity: base.href, close: () => agent.destroy() };
}

function responseError(status) {
    return new Error('Gateway returned HTTP ' + status);
}

function validPage(body, topic, start) {
    if (body?.topic !== topic || body.partition !== 0 || !offset(body.nextOffset)
        || !offset(body.endOffset) || BigInt(body.nextOffset) > BigInt(body.endOffset)
        || !Array.isArray(body.records)) return false;
    let previous = start === undefined ? -1n : BigInt(start) - 1n;
    for (const record of body.records) {
        if (!offset(record?.offset)) return false;
        const current = BigInt(record.offset);
        if (current <= previous || current >= BigInt(body.nextOffset)
            || !(Object.hasOwn(record, 'value') || typeof record.valueBase64 === 'string')) return false;
        previous = current;
    }
    return start === undefined || BigInt(body.nextOffset) >= BigInt(start);
}

module.exports = function register(RED) {
    function Gateway(config) {
        RED.nodes.createNode(this, config);
        try {
            this.client = createClient(config, this.credentials);
        } catch (error) {
            this.error(error.message);
        }
        this.on('close', () => this.client?.close());
    }

    function Producer(config) {
        RED.nodes.createNode(this, config);
        const client = RED.nodes.getNode(config.gateway)?.client;
        let pending = 0;
        let queue = Promise.resolve();
        const publish = async (msg, send, done) => {
            try {
                if (!client) throw new Error('Select a valid Kafka HTTP gateway');
                // A topic set on the node takes precedence; otherwise msg.topic selects it.
                const topic = topicName(config.topic || msg.topic);
                const { status, body } = await client.request('POST', topic, { value: msg.payload });
                if (status !== 201) throw responseError(status);
                if (body?.topic !== topic || body.key !== topic || body.partition !== 0 || !offset(body.offset)) {
                    throw new Error('Gateway returned an invalid publish receipt');
                }
                msg.kafka = { topic, partition: body.partition, key: body.key, offset: body.offset };
                if (pending === 1) this.status({ fill: 'green', shape: 'dot', text: 'offset ' + body.offset });
                send(msg);
                done();
            } catch (error) {
                this.status({ fill: 'red', shape: 'ring', text: 'publish failed' });
                done(error);
            } finally {
                pending -= 1;
            }
        };
        // Publish one message at a time so records reach the topic in arrival order.
        this.on('input', (msg, send, done) => {
            pending += 1;
            if (pending > 1) this.status({ fill: 'blue', shape: 'dot', text: pending + ' queued' });
            queue = queue.then(() => publish(msg, send, done));
        });
        this.on('close', () => this.status({}));
    }

    function Consumer(config) {
        RED.nodes.createNode(this, config);
        const client = RED.nodes.getNode(config.gateway)?.client;
        let topic, interval, limit;
        try {
            if (!client) throw new Error('Select a valid Kafka HTTP gateway');
            topic = topicName(config.topic);
            interval = integer(config.interval || 5000, 500, 600000, 'Poll interval');
            limit = integer(config.limit || 100, 1, 100, 'Page size');
        } catch (error) {
            this.status({ fill: 'red', shape: 'ring', text: 'configuration error' });
            this.error(error.message);
            return;
        }
        const state = this.context();
        const source = JSON.stringify([config.gateway, client.identity, topic]);
        const cursorKey = 'kafkaHttpOffset_' + createHash('sha256').update(source).digest('hex');
        // Callback form works with both synchronous and asynchronous context stores.
        const load = () => new Promise((resolve, reject) => {
            state.get(cursorKey, (error, value) => (error ? reject(error) : resolve(value)));
        });
        const save = value => new Promise((resolve, reject) => {
            state.set(cursorKey, value, error => (error ? reject(error) : resolve()));
        });
        let next;
        let pageSize = limit;
        let loaded = false;
        let timer;
        let closing = false;
        let previousError;
        this.on('close', () => {
            closing = true;
            clearTimeout(timer);
        });

        // Halve the page while a response is over the size limit, so large records cannot block the cursor.
        const readPage = async query => {
            for (;;) {
                try {
                    return await client.request('GET', topic, { query });
                } catch (error) {
                    if (error.code !== responseTooLarge) throw error;
                    if (query.limit === 1) {
                        throw new Error(error.message + ' with a single record. Raise the maximum response size of the gateway.');
                    }
                    query.limit = Math.ceil(query.limit / 2);
                    if (closing) throw error;
                }
            }
        };

        const fetchPage = async () => {
            let delay = interval;
            try {
                if (!loaded) {
                    const stored = await load();
                    if (closing) return;
                    next = offset(stored) ? stored : undefined;
                    loaded = true;
                }
                const initialLatest = next === undefined && config.start !== 'earliest';
                const query = { limit: initialLatest ? 1 : pageSize };
                if (next !== undefined) query.offset = next;
                const { status, body } = await readPage(query);
                if (closing) return;
                if (status === 410 && offset(body?.earliestOffset)) {
                    this.warn('Offset expired. Resuming from the earliest retained record.');
                    next = body.earliestOffset;
                } else if (status === 416 && offset(body?.endOffset)) {
                    this.warn('Offset is beyond the end. Resuming from the current end.');
                    next = body.endOffset;
                } else {
                    if (status !== 200) throw responseError(status);
                    if (!validPage(body, topic, next)) throw new Error('Gateway returned an invalid page');
                    if (!initialLatest) {
                        for (const record of body.records) {
                            this.send({
                                topic,
                                payload: Object.hasOwn(record, 'value')
                                    ? record.value : Buffer.from(record.valueBase64, 'base64'),
                                kafka: { topic, partition: 0, key: record.key, offset: record.offset, timestamp: record.timestamp },
                            });
                        }
                    }
                    next = initialLatest ? body.endOffset : body.nextOffset;
                    if (!initialLatest) {
                        pageSize = query.limit;
                        // A full page means more records are probably waiting: read again without waiting.
                        if (body.records.length === query.limit && next !== body.endOffset) delay = 0;
                        else pageSize = limit;
                    }
                }
                await save(next);
                previousError = undefined;
                this.status({ fill: 'green', shape: 'dot', text: 'offset ' + next });
            } catch (error) {
                if (closing) return;
                this.status({ fill: 'red', shape: 'ring', text: 'read failed' });
                if (error.message !== previousError) this.error(error.message);
                previousError = error.message;
            } finally {
                if (!closing) timer = setTimeout(fetchPage, delay);
            }
        };
        this.status({ fill: 'yellow', shape: 'ring', text: 'connecting' });
        void fetchPage();
    }

    RED.nodes.registerType('kafka-http-gateway-config', Gateway, {
        credentials: { username: { type: 'text' }, password: { type: 'password' } },
    });
    RED.nodes.registerType('kafka-http-producer', Producer);
    RED.nodes.registerType('kafka-http-consumer', Consumer);
};
