# @blokcert/node-red-contrib-delayed-job

A Node-RED node for Redis-backed delayed job queues using [BullMQ](https://docs.bullmq.io/).

Each node acts as both **producer** and **consumer** — messages are serialized into a Redis queue on input, and a background worker picks them up and sends them to the output.

## Features

- **Delayed execution** — schedule jobs to run after a specified delay
- **Job priority** — lower number = higher priority
- **TTL (Time-to-live)** — automatically discard expired jobs
- **Rate limiting** — control the interval between job processing
- **Concurrency** — process multiple jobs simultaneously
- **Crash recovery** — jobs persist in Redis; restarting Node-RED picks up unfinished jobs
- **Distributed workers** — multiple Node-RED instances sharing the same queue name form a worker pool
- **Buffer support** — `Buffer` objects are preserved through serialization
- **i18n** — English and Traditional Chinese (zh-TW)

## Install

```bash
cd ~/.node-red
npm install @blokcert/node-red-contrib-delayed-job
```

Requires a running Redis server (or compatible, e.g. Valkey, Dragonfly).

## Nodes

### delayed-job

The main node. Drag it into your flow, configure the Redis connection, and wire it up.

**Input:** messages are serialized and added to the Redis queue.

**Output:** when a job is picked up by the worker, the deserialized message is sent out.

#### Node Settings

| Setting | Default | Description |
|---|---|---|
| Redis | (required) | Redis connection config |
| Concurrency | 1 | Number of jobs processed simultaneously |
| Interval | 0 ms | Minimum time between job processing (rate limit) |

#### Message Properties

| Property | Type | Description |
|---|---|---|
| `msg.delay` | number | Delay in milliseconds before the job is processed |
| `msg.priority` | number | Job priority (lower = higher priority) |
| `msg.ttl` | number | Time-to-live in seconds. Expired jobs are discarded. Default: 7 days |
| `msg.keepAfter` | number | Keep job in Redis for N seconds after completion. Default: 7 days |

### delayed-job-redis (Config Node)

Shared Redis connection configuration.

| Setting | Default | Description |
|---|---|---|
| Host | 127.0.0.1 | Redis server hostname |
| Port | 6379 | Redis server port |
| Password | — | Authentication password (stored encrypted) |
| DB | 0 | Redis database index |
| TLS | off | Enable TLS/SSL |
| Queue Name | default | Queue name prefix. Same name = shared queue across instances |

## How It Works

```
[inject] → [delayed-job] → [debug]
              ↕ Redis
```

1. A message arrives at the input
2. The node serializes it (removing `_msgid`) and adds it to the BullMQ queue in Redis
3. The background worker picks up the job, deserializes the message, and sends it to the output
4. Node-RED assigns a new `_msgid`

### Queue Isolation

Each node instance gets a unique queue: `{queueName}-{nodeId}`. To share a queue across multiple Node-RED instances (distributed worker pool), use the same Queue Name in the Redis config.

### Crash Recovery

Jobs are stored in Redis and retained for 7 days after completion by default. If Node-RED crashes or restarts, the worker will pick up any remaining jobs automatically.

### Limitations

- `msg.req` and `msg.res` (HTTP request/response objects) cannot be serialized and will be lost
- Non-JSON-serializable properties will be dropped

## Example Flow

```json
[
  {
    "id": "inject1",
    "type": "inject",
    "payload": "hello",
    "wires": [["delayed1"]]
  },
  {
    "id": "delayed1",
    "type": "delayed-job",
    "name": "my queue",
    "wires": [["debug1"]]
  },
  {
    "id": "debug1",
    "type": "debug",
    "name": "output"
  }
]
```

## Testing

Requires a local Redis server running.

```bash
npm test
```

Set `REDIS_HOST` and `REDIS_PORT` environment variables to use a non-default Redis server.

## License

MIT
