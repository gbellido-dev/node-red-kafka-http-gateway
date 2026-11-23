# Kafka HTTP Gateway nodes for Node-RED

Read and publish Kafka records through an HTTPS gateway with an offset-based JSON API. The package adds **kafka http consumer** and **kafka http producer** to the network category.

## Install and configure

1. Use Node.js 20 or later and Node-RED 4 or later. In **Manage palette → Install**, search for `@gbellido/node-red-kafka-http-gateway`. To install from a terminal, run `npm install @gbellido/node-red-kafka-http-gateway` in your Node-RED user directory and restart Node-RED.
2. Add a consumer or producer. Create a shared gateway configuration with your HTTPS base URL and the HTTP Basic username and password if required by the gateway.
3. Set one topic per consumer or producer. To read, connect the consumer to a Debug node. To publish, wire a message containing the JSON value in `msg.payload` into the producer.

The optional **Connect IP** setting overrides DNS resolution while retaining the certificate hostname and HTTP host. TLS verification is always enabled and credentials are stored as Node-RED credentials.

The consumer keeps its next offset in Node-RED node context. Choose **New records** to start at the end of the topic when there is no stored cursor, or **Earliest retained record** to read existing records. Configure persistent Node-RED context if offsets should survive a runtime restart. The producer adds a receipt to `msg.kafka` only after the gateway returns HTTP 201. A failed request may have reached the broker, so replaying a failed publish can create a duplicate.

## Gateway API

The server must implement the following contract over HTTPS. These nodes are clients of this contract. Native Kafka brokers and Confluent REST Proxy expose different protocols or routes.

| Method | Route | Contract |
|---|---|---|
| GET | `/kafka/v1/topics/<topic>/messages` | Accepts optional `offset` and `limit`. HTTP 200 returns `topic`, `partition`, `endOffset`, `nextOffset`, and `records`. Each record contains `key`, `offset`, `timestamp`, and `value` as JSON or `valueBase64` for raw bytes. |
| POST | `/kafka/v1/topics/<topic>/messages` | Accepts `{"value": <JSON value>}` and returns HTTP 201 with `topic`, `partition`, `key`, and `offset`. The gateway uses the topic name as the record key. |

Offsets are nonnegative decimal strings, and `endOffset` is exclusive. A successful empty page keeps the requested offset as `nextOffset`. On HTTP 410, the gateway returns `earliestOffset` and the consumer resumes there. On HTTP 416, the gateway returns `endOffset` and the consumer resumes at the current end.

This release supports one partition per topic, with partition ID 0. Topic names contain lowercase ASCII letters, digits and hyphens. The consumer defaults to up to 100 records every five seconds, and POST JSON bodies are limited to 128 KiB. Each output record has the value in `msg.payload` and metadata in `msg.kafka`. Raw values are emitted as a Buffer.

## Publish this package

Run `npm test` and `npm pack --dry-run` before release. Tests use OpenSSL to generate a temporary certificate. Set `KAFKA_HTTP_TEST_OPENSSL` to the OpenSSL executable if it is outside your PATH. Publish with an npm account that can publish to `@gbellido`, using `npm publish --access public`. Submit the exact package name to the [Node-RED Flow Library](https://flows.nodered.org/add/node).
