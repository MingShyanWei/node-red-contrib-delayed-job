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

    // --- ACK mode ---
    const completionMode = config.completionMode || 'auto';
    const ackMode = completionMode === 'ack';

    const ackTimeoutRaw = parseInt(config.ackTimeoutValue, 10) || 30;
    const ackTimeoutUnit = config.ackTimeoutUnit || 's';
    const ackUnitMultiplier = { s: 1000, m: 60000, h: 3600000 };
    const ackTimeoutMs = ackTimeoutRaw * (ackUnitMultiplier[ackTimeoutUnit] || 1000);

    const ackMaxRetries = parseInt(config.ackMaxRetries, 10) || 3;

    // --- Verbose / Debug logging ---
    const verbose = config.verbose === true || config.verbose === 'true';
    function debug(msg) {
      if (verbose) node.warn('[DEBUG] ' + msg);
    }

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

    // pendingJobs: jobId -> { job, token, resolve, reject, timer, attempt, jobData, groupId }
    const pendingJobs = new Map();
    // groupMap: groupId -> Set<jobId>  (tracks all jobIds belonging to one logical job)
    const groupMap = new Map();
    // completedGroups: Set<groupId> (groups that have been ACK'd — skip future retries)
    const completedGroups = new Set();

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

    // --- Send job to fault output (output 2) ---

    function sendToFault(jobData, jobId, attempt, errorMsg) {
      const msg = deserializeMsg(jobData);
      delete msg._ttl;
      delete msg._ackAttempt;
      delete msg._ackGroupId;
      msg._jobId = jobId;
      msg._jobFailed = true;
      msg._jobError = errorMsg;
      msg._jobAttempt = attempt;
      node.send([null, msg]);
    }

    // --- Release a pending job from BullMQ ---

    function releasePending(pending) {
      if (pending.resolve) {
        pending.resolve();
      } else if (pending.job) {
        pending.job.moveToCompleted(undefined, pending.token, false).catch(() => {});
      }
    }

    // --- Cancel a single pending job (clear timer, release, remove from maps) ---

    function cancelPendingJob(jobId) {
      const pending = pendingJobs.get(jobId);
      if (!pending) return;
      clearTimeout(pending.timer);
      pendingJobs.delete(jobId);
      releasePending(pending);
    }

    // --- Cancel all pending jobs in a group ---

    function cancelGroup(groupId) {
      const jobIds = groupMap.get(groupId);
      if (!jobIds) return;
      for (const jid of jobIds) {
        cancelPendingJob(jid);
      }
      groupMap.delete(groupId);
      completedGroups.add(groupId);
      // Clean up completedGroups after a while to prevent memory leak
      setTimeout(() => completedGroups.delete(groupId), ackTimeoutMs * (ackMaxRetries + 2));
    }

    // --- Remove a jobId from its group ---

    function removeFromGroup(jobId, groupId) {
      const jobIds = groupMap.get(groupId);
      if (jobIds) {
        jobIds.delete(jobId);
        if (jobIds.size === 0) groupMap.delete(groupId);
      }
    }

    // --- ACK timeout handler: re-queue or fault ---

    function handleAckTimeout(jobId) {
      const pending = pendingJobs.get(jobId);
      if (!pending) return;

      const groupId = pending.groupId;

      // If this group was already ACK'd by an earlier attempt, just clean up
      if (completedGroups.has(groupId)) {
        debug('Job ' + jobId + ' timeout ignored (group already ACK\'d)');
        cancelPendingJob(jobId);
        removeFromGroup(jobId, groupId);
        return;
      }

      pendingJobs.delete(jobId);
      removeFromGroup(jobId, groupId);

      const attempt = pending.attempt + 1;

      releasePending(pending);

      // Check if we should retry or fault
      if (attempt >= ackMaxRetries) {
        // Max retries exhausted → cancel entire group and send to fault
        debug('Job ' + jobId + ' failed after ' + attempt + ' attempt(s)');
        // Cancel any other pending jobs in this group
        const remainingJobs = groupMap.get(groupId);
        if (remainingJobs) {
          for (const jid of remainingJobs) {
            cancelPendingJob(jid);
          }
          groupMap.delete(groupId);
        }
        completedGroups.add(groupId);
        setTimeout(() => completedGroups.delete(groupId), ackTimeoutMs * 2);
        sendToFault(pending.jobData, jobId, attempt, 'ACK timeout after ' + attempt + ' attempt(s)');
      } else {
        // Re-queue the job for retry
        debug('Job ' + jobId + ' ACK timeout (attempt ' + attempt + '/' + ackMaxRetries + '), re-queuing');
        const retryData = Object.assign({}, pending.jobData);
        retryData._ackAttempt = attempt;
        retryData._ackGroupId = groupId;
        queue.add('job', retryData, { attempts: 1 }).catch((err) => {
          node.error('Failed to re-queue job: ' + err.message);
        });
      }
    }

    // --- Public ACK handler (called by job-ack node directly) ---

    node.handleAck = async function (jobId, success, errorMsg) {
      jobId = String(jobId);
      const pending = pendingJobs.get(jobId);

      if (!pending) {
        debug('ACK for unknown or already-completed job: ' + jobId);
        return;
      }

      const groupId = pending.groupId;

      clearTimeout(pending.timer);
      pendingJobs.delete(jobId);
      removeFromGroup(jobId, groupId);

      if (success) {
        debug('Job ' + jobId + ' ACK success, cancelling group ' + groupId);
        releasePending(pending);
        // Cancel all other pending jobs in this group (retries no longer needed)
        cancelGroup(groupId);
      } else {
        releasePending(pending);
        // Cancel all other pending jobs in this group
        cancelGroup(groupId);
        sendToFault(pending.jobData, jobId, pending.attempt, errorMsg || 'Explicitly failed by ACK');
      }
    };

    // --- Register a pending job in maps ---

    function registerPending(jobId, pendingEntry) {
      const groupId = pendingEntry.groupId;
      pendingJobs.set(jobId, pendingEntry);
      if (!groupMap.has(groupId)) {
        groupMap.set(groupId, new Set());
      }
      groupMap.get(groupId).add(jobId);
    }

    // --- Job processor (shared between both modes) ---

    function processJob(job) {
      const ttlMs = job.data._ttl;
      if (ttlMs && Date.now() - job.timestamp > ttlMs) {
        debug('Job ' + job.id + ' expired (TTL ' + (ttlMs / 1000) + 's)');
        return false;
      }

      // Check if this group was already completed
      const groupId = job.data._ackGroupId;
      if (groupId && completedGroups.has(groupId)) {
        debug('Job ' + job.id + ' skipped (group already ACK\'d)');
        return false;
      }

      const msg = deserializeMsg(job.data);
      delete msg._ttl;
      delete msg._ackAttempt;
      delete msg._ackGroupId;
      lastProcessedAt = Date.now();

      if (ackMode) {
        msg._jobId = job.id;
        msg._jobAttempt = job.data._ackAttempt || 0;
        msg._jobSourceNode = node.id;
      }

      node.send([msg, null]);
      return true;
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
            lockDuration: ackMode ? Math.max(ackTimeoutMs + 5000, 30000) : 30000,
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

          worker.startStalledCheckTimer();
          node.status({ fill: 'green', shape: 'dot', text: 'ready' });

          fetchTimer = setInterval(async () => {
            if (closing) return;
            try {
              for (let i = 0; i < node.concurrency; i++) {
                if (closing) break;
                const job = await worker.getNextJob(workerToken, { block: false });
                if (!job) break;
                try {
                  const processed = processJob(job);
                  if (ackMode && processed) {
                    const attempt = job.data._ackAttempt || 0;
                    const groupId = job.data._ackGroupId || crypto.randomUUID();
                    const timer = setTimeout(() => handleAckTimeout(job.id), ackTimeoutMs);
                    registerPending(job.id, {
                      job,
                      token: workerToken,
                      timer,
                      attempt,
                      jobData: job.data,
                      groupId,
                    });
                  } else {
                    await job.moveToCompleted(undefined, workerToken, false);
                  }
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
            async (job, token) => {
              if (closing) return;

              if (ackMode) {
                return new Promise((resolve, reject) => {
                  const processed = processJob(job);
                  if (processed) {
                    const attempt = job.data._ackAttempt || 0;
                    const groupId = job.data._ackGroupId || crypto.randomUUID();
                    const timer = setTimeout(() => handleAckTimeout(job.id), ackTimeoutMs);
                    registerPending(job.id, {
                      job,
                      token,
                      resolve,
                      reject,
                      timer,
                      attempt,
                      jobData: job.data,
                      groupId,
                    });
                  } else {
                    resolve();
                  }
                });
              } else {
                processJob(job);
              }
            },
            {
              connection: connectionOpts,
              concurrency: node.concurrency,
              lockDuration: ackMode ? Math.max(ackTimeoutMs + 5000, 30000) : 30000,
              stalledInterval: 30000,
              maxStalledCount: 2,
            }
          );

          worker.on('ready', () => {
            node.status({ fill: 'green', shape: 'dot', text: 'ready' });
          });

          worker.on('failed', (job, err) => {
            debug('Job ' + (job ? job.id : '?') + ' failed: ' + err.message);
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
            debug('Job stalled: ' + jobId);
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

          if (total === 0 && pendingJobs.size === 0) {
            node.status({ fill: 'green', shape: 'dot', text: 'idle' });
          } else if (intervalMs > 0 && lastProcessedAt > 0) {
            const elapsed = Date.now() - lastProcessedAt;
            const remaining = Math.max(0, intervalMs - elapsed);
            const remainSec = Math.ceil(remaining / 1000);
            const parts = [];
            if (remaining > 0 && waiting > 0) {
              parts.push(waiting + ' waiting | next: ' + remainSec + 's');
            } else {
              if (active > 0) parts.push(active + ' active');
              if (waiting > 0) parts.push(waiting + ' waiting');
            }
            if (ackMode && pendingJobs.size > 0) {
              parts.push(pendingJobs.size + ' pending ACK');
            }
            node.status({ fill: 'blue', shape: 'dot', text: parts.join(' | ') });
          } else {
            const parts = [];
            if (active > 0) parts.push(active + ' active');
            if (waiting > 0) parts.push(waiting + ' waiting');
            if (ackMode && pendingJobs.size > 0) {
              parts.push(pendingJobs.size + ' pending ACK');
            }
            node.status({ fill: 'blue', shape: 'dot', text: parts.join(' | ') });
          }
        } catch (_) { /* ignore status errors */ }
      }

      statusTimer = setInterval(updateStatus, 1000);
    })();

    // --- Input Handler (Producer + ACK) ---

    node.on('input', async function (msg, send, done) {
      send = send || function () { node.send.apply(node, arguments); };

      if (closing) {
        if (done) done(new Error('Node is closing, job not queued'));
        return;
      }

      // --- ACK handling (wire-back compatibility) ---
      if (ackMode && msg._jobAck !== undefined && msg._jobId) {
        const success = msg._jobAck === true || msg._jobAck === 'true';
        await node.handleAck(msg._jobId, success, msg._jobError);
        if (done) done();
        return;
      }

      // --- Normal job enqueuing ---
      try {
        const ttlSec = parseInt(msg.ttl, 10) || 604800;
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

      for (const [jobId, pending] of pendingJobs) {
        clearTimeout(pending.timer);
        if (pending.resolve) {
          pending.resolve();
        }
      }
      pendingJobs.clear();
      groupMap.clear();
      completedGroups.clear();

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
