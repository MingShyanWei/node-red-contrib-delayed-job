const helper = require('node-red-node-test-helper');
const delayedJobNode = require('../delayed-job/delayed-job.js');
const redisConfigNode = require('../delayed-job/delayed-job-redis.js');
const { Queue } = require('bullmq');
const { expect } = require('chai');

helper.init(require.resolve('node-red'));

describe('delayed-job Node', function () {
  afterEach(async function () {
    await helper.unload();
  });

  const redisHost = process.env.REDIS_HOST || '127.0.0.1';
  const redisPort = parseInt(process.env.REDIS_PORT, 10) || 6379;

  function baseFlow(queueName) {
    return [
      {
        id: 'rc1',
        type: 'delayed-job-redis',
        host: redisHost,
        port: redisPort,
        db: 0,
        tls: false,
        queueName: queueName,
      },
      {
        id: 'n1',
        type: 'delayed-job',
        name: 'test-job',
        redis: 'rc1',
        concurrency: 1,
        wires: [['out1']],
      },
      { id: 'out1', type: 'helper' },
    ];
  }

  async function cleanQueue(queueName) {
    const fullName = queueName + '-n1';
    const q = new Queue(fullName, {
      connection: { host: redisHost, port: redisPort, maxRetriesPerRequest: null },
    });
    await q.obliterate({ force: true });
    await q.close();
  }

  it('should load both nodes', function (done) {
    const flow = baseFlow('load-test');
    helper.load([redisConfigNode, delayedJobNode], flow, function () {
      const n1 = helper.getNode('n1');
      expect(n1).to.have.property('name', 'test-job');
      done();
    });
  });

  it('should round-trip a simple message', function (done) {
    const queueName = 'roundtrip-test-' + Date.now();
    const flow = baseFlow(queueName);

    helper.load([redisConfigNode, delayedJobNode], flow, function () {
      const n1 = helper.getNode('n1');
      const out1 = helper.getNode('out1');

      out1.on('input', function (msg) {
        try {
          expect(msg.payload).to.equal('hello');
          expect(msg.custom).to.equal(42);
          cleanQueue(queueName).then(() => done());
        } catch (err) {
          done(err);
        }
      });

      n1.receive({ payload: 'hello', custom: 42 });
    });
  });

  it('should preserve Buffer payloads', function (done) {
    const queueName = 'buffer-test-' + Date.now();
    const flow = baseFlow(queueName);

    helper.load([redisConfigNode, delayedJobNode], flow, function () {
      const n1 = helper.getNode('n1');
      const out1 = helper.getNode('out1');

      const original = Buffer.from('binary data');

      out1.on('input', function (msg) {
        try {
          expect(Buffer.isBuffer(msg.payload)).to.be.true;
          expect(msg.payload.toString()).to.equal('binary data');
          cleanQueue(queueName).then(() => done());
        } catch (err) {
          done(err);
        }
      });

      n1.receive({ payload: original });
    });
  });

  it('should handle delayed jobs', function (done) {
    this.timeout(5000);
    const queueName = 'delay-test-' + Date.now();
    const flow = baseFlow(queueName);

    helper.load([redisConfigNode, delayedJobNode], flow, function () {
      const n1 = helper.getNode('n1');
      const out1 = helper.getNode('out1');

      const start = Date.now();

      out1.on('input', function (msg) {
        try {
          const elapsed = Date.now() - start;
          expect(elapsed).to.be.at.least(900);
          expect(msg.payload).to.equal('delayed');
          cleanQueue(queueName).then(() => done());
        } catch (err) {
          done(err);
        }
      });

      n1.receive({ payload: 'delayed', delay: 1000 });
    });
  });

  it('should handle object payloads with nested properties', function (done) {
    const queueName = 'object-test-' + Date.now();
    const flow = baseFlow(queueName);

    helper.load([redisConfigNode, delayedJobNode], flow, function () {
      const n1 = helper.getNode('n1');
      const out1 = helper.getNode('out1');

      const original = {
        payload: {
          name: 'test',
          items: [1, 2, 3],
          nested: { deep: true },
        },
        topic: 'my-topic',
      };

      out1.on('input', function (msg) {
        try {
          expect(msg.payload).to.deep.equal(original.payload);
          expect(msg.topic).to.equal('my-topic');
          cleanQueue(queueName).then(() => done());
        } catch (err) {
          done(err);
        }
      });

      n1.receive(original);
    });
  });
});
