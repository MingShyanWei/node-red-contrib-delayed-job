module.exports = function (RED) {
  const { Queue, Worker } = require('bullmq');
  const crypto = require('crypto');

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
    let statusTimer = null;
    let fetchTimer = null;
    let lastProcessedAt = 0;
    const workerToken = crypto.randomUUID();

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

    // --- Job processor (shared between both modes) ---

    function processJob(job) {
      const ttlMs = job.data._ttl;
      if (ttlMs && Date.now() - job.timestamp > ttlMs) {
        node.warn('Job ' + job.id + ' expired (TTL ' + (ttlMs / 1000) + 's)');
        return;
      }
      const msg = deserializeMsg(job.data);
      delete msg._ttl;
      lastProcessedAt = Date.now();
      node.send(msg);
    }

    // --- Initialize Queue & Worker ---

    (async function init() {
      try {
        queue = new Queue(node.queueName, {
          connection: connectionOpts,
          defaultJobOptions: {
            removeOnComplete: { age: 604800 },
            removeOnFail: { age: 604800 },
          },
        });
      } catch (err) {
        node.error('Failed to create queue: ' + err.message);
        node.status({ fill: 'red', shape: 'ring', text: 'queue init fail' });
        return;
      }

      try {
        if (intervalMs > 0) {
          // --- Manual mode: per-worker rate limiting ---
          worker = new Worker(node.queueName, null, {
            connection: connectionOpts,
            autorun: false,
            lockDuration: 30000,
            stalledInterval: 30000,
            maxStalledCount: 2,
          });

          worker.on('error', (err) => {
            node.error('Worker error: ' + err.message);
            node.status({
              fill: 'red',
              shape: 'ring',
              text: err.message.substring(0, 30),
            });
          });

          // Start stalled job checker for crash recovery
          worker.startStalledCheckTimer();

          node.status({ fill: 'green', shape: 'dot', text: 'ready' });

          // Fetch jobs on interval
          fetchTimer = setInterval(async () => {
            if (closing) return;
            try {
              for (let i = 0; i < node.concurrency; i++) {
                if (closing) break;
                const job = await worker.getNextJob(workerToken, { block: false });
                if (!job) break; // no more jobs available
                try {
                  processJob(job);
                  await job.moveToCompleted(undefined, workerToken, false);
                } catch (err) {
                  node.error('Job ' + job.id + ' failed: ' + err.message);
                  await job.moveToFailed(err, workerToken, false);
                }
              }
            } catch (err) {
              if (!closing) {
                node.error('Error fetching jobs: ' + err.message);
              }
            }
          }, intervalMs);

        } else {
          // --- Automatic mode: immediate processing ---
          worker = new Worker(
            node.queueName,
            async (job) => {
              if (closing) return;
              processJob(job);
            },
            {
              connection: connectionOpts,
              concurrency: node.concurrency,
              stalledInterval: 30000,
              maxStalledCount: 2,
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
        }
      } catch (err) {
        node.error('Failed to create worker: ' + err.message);
        node.status({ fill: 'red', shape: 'ring', text: 'worker init fail' });
        return;
      }

      // --- Status update timer ---
      async function updateStatus() {
        if (closing || !queue) return;
        try {
          const counts = await queue.getJobCounts('waiting', 'delayed', 'active');
          const waiting = (counts.waiting || 0) + (counts.delayed || 0);
          const active = counts.active || 0;
          const total = waiting + active;

          if (total === 0) {
            node.status({ fill: 'green', shape: 'dot', text: 'idle' });
          } else if (intervalMs > 0 && lastProcessedAt > 0) {
            const elapsed = Date.now() - lastProcessedAt;
            const remaining = Math.max(0, intervalMs - elapsed);
            const remainSec = Math.ceil(remaining / 1000);
            if (remaining > 0 && waiting > 0) {
              node.status({ fill: 'blue', shape: 'dot', text: waiting + ' waiting | next: ' + remainSec + 's' });
            } else {
              const parts = [];
              if (active > 0) parts.push(active + ' active');
              if (waiting > 0) parts.push(waiting + ' waiting');
              node.status({ fill: 'blue', shape: 'dot', text: parts.join(' | ') });
            }
          } else {
            const parts = [];
            if (active > 0) parts.push(active + ' active');
            if (waiting > 0) parts.push(waiting + ' waiting');
            node.status({ fill: 'blue', shape: 'dot', text: parts.join(' | ') });
          }
        } catch (_) { /* ignore status errors */ }
      }

      statusTimer = setInterval(updateStatus, 1000);
    })();

    // --- Input Handler (Producer) ---

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };

      if (closing) {
        if (done) done(new Error('Node is closing, job not queued'));
        return;
      }

      try {
        const ttlSec = parseInt(msg.ttl, 10) || 604800; // default 7 days
        const serialized = serializeMsg(msg);
        serialized._ttl = ttlSec * 1000;
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
      if (fetchTimer) {
        clearInterval(fetchTimer);
        fetchTimer = null;
      }
      if (statusTimer) {
        clearInterval(statusTimer);
        statusTimer = null;
      }
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
