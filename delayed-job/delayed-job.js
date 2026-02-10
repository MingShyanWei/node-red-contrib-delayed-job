module.exports = function (RED) {
  const { Queue, Worker } = require('bullmq');

  function DelayedJobNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.concurrency = parseInt(config.concurrency, 10) || 1;

    // --- Rate limit (interval between jobs) ---
    const raw = parseInt(config.intervalValue, 10);
    const intervalValue = isNaN(raw) ? 0 : raw;
    const intervalUnit = config.intervalUnit || 'ms';
    const unitMultiplier = { ms: 1, s: 1000, m: 60000, h: 3600000 };
    const intervalMs = intervalValue * (unitMultiplier[intervalUnit] || 1);

    node.redisConfig = RED.nodes.getNode(config.redis);
    if (!node.redisConfig) {
      node.status({ fill: 'red', shape: 'ring', text: 'no redis config' });
      node.error('No Redis configuration node selected');
      return;
    }

    const prefix = node.redisConfig.queueName || 'default';
    node.queueName = prefix + '-' + node.id;

    const connectionOpts = node.redisConfig.connectionOptions;
    let queue = null;
    let worker = null;
    let closing = false;

    // --- Serialization helpers ---

    function serializeMsg(msg) {
      const clean = Object.assign({}, msg);
      delete clean._msgid;
      return JSON.parse(JSON.stringify(clean));
    }

    function deserializeMsg(data) {
      return reviveBuffers(data);
    }

    function reviveBuffers(obj) {
      if (obj === null || typeof obj !== 'object') return obj;
      if (obj.type === 'Buffer' && Array.isArray(obj.data)) {
        return Buffer.from(obj.data);
      }
      if (Array.isArray(obj)) {
        return obj.map(reviveBuffers);
      }
      for (const key of Object.keys(obj)) {
        obj[key] = reviveBuffers(obj[key]);
      }
      return obj;
    }

    // --- Initialize Queue (Producer) ---

    try {
      queue = new Queue(node.queueName, {
        connection: connectionOpts,
        defaultJobOptions: {
          removeOnComplete: false,
          removeOnFail: false,
        },
      });
    } catch (err) {
      node.error('Failed to create queue: ' + err.message);
      node.status({ fill: 'red', shape: 'ring', text: 'queue init fail' });
      return;
    }

    // --- Initialize Worker (Consumer) ---

    try {
      worker = new Worker(
        node.queueName,
        async (job) => {
          if (closing) return;
          const ttlMs = job.data._ttl;
          if (ttlMs && Date.now() - job.timestamp > ttlMs) {
            node.warn('Job ' + job.id + ' expired (TTL ' + (ttlMs / 1000) + 's)');
            return;
          }
          const msg = deserializeMsg(job.data);
          delete msg._ttl;
          node.send(msg);
        },
        {
          connection: connectionOpts,
          concurrency: node.concurrency,
          stalledInterval: 30000,
          maxStalledCount: 2,
          ...(intervalMs > 0 ? { limiter: { max: node.concurrency, duration: intervalMs } } : {}),
        }
      );

      worker.on('ready', () => {
        node.status({ fill: 'green', shape: 'dot', text: 'ready' });
      });

      worker.on('failed', (job, err) => {
        node.warn('Job ' + (job ? job.id : '?') + ' failed: ' + err.message);
      });

      worker.on('error', (err) => {
        node.error('Worker error: ' + err.message);
        node.status({
          fill: 'red',
          shape: 'ring',
          text: err.message.substring(0, 30),
        });
      });

      worker.on('stalled', (jobId) => {
        node.warn('Job stalled: ' + jobId);
      });
    } catch (err) {
      node.error('Failed to create worker: ' + err.message);
      node.status({ fill: 'red', shape: 'ring', text: 'worker init fail' });
      return;
    }

    // --- Input Handler (Producer) ---

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };

      if (closing) {
        if (done) done(new Error('Node is closing, job not queued'));
        return;
      }

      try {
        const ttlSec = parseInt(msg.ttl, 10) || 0;
        const serialized = serializeMsg(msg);
        if (ttlSec > 0) {
          serialized._ttl = ttlSec * 1000;
        }
        const jobName = 'job';
        const jobOpts = {};

        const delay = parseInt(msg.delay, 10) || 0;
        if (delay > 0) {
          jobOpts.delay = delay;
        }
        if (msg.priority !== undefined) {
          jobOpts.priority = parseInt(msg.priority, 10);
        }
        const keepAfterSec = parseInt(msg.keepAfter, 10) || 0;
        if (keepAfterSec > 0) {
          jobOpts.removeOnComplete = { age: keepAfterSec };
          jobOpts.removeOnFail = { age: keepAfterSec };
        }
        await queue.add(jobName, serialized, jobOpts);

        if (done) done();
      } catch (err) {
        if (done) {
          done(err);
        } else {
          node.error(err, msg);
        }
      }
    });

    // --- Close Handler (Graceful Shutdown) ---

    node.on('close', async function (removed, done) {
      closing = true;
      node.status({ fill: 'yellow', shape: 'ring', text: 'closing...' });

      try {
        if (worker) {
          await worker.close();
        }
        if (queue) {
          await queue.close();
        }
      } catch (err) {
        node.error('Error during shutdown: ' + err.message);
      }

      node.status({});
      done();
    });
  }

  RED.nodes.registerType('delayed-job', DelayedJobNode);
};
