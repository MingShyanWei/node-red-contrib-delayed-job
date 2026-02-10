module.exports = function (RED) {
  function RedisConfigNode(config) {
    RED.nodes.createNode(this, config);

    this.host = config.host || '127.0.0.1';
    this.port = parseInt(config.port, 10) || 6379;
    this.db = parseInt(config.db, 10) || 0;
    this.tls = config.tls || false;
    this.queueName = config.queueName || 'default';

    const password = this.credentials.password || undefined;

    this.connectionOptions = {
      host: this.host,
      port: this.port,
      db: this.db,
      password: password,
      maxRetriesPerRequest: null,
      enableReadyCheck: false,
    };

    if (this.tls) {
      this.connectionOptions.tls = {};
    }

    this.on('close', function (done) {
      done();
    });
  }

  RED.nodes.registerType('delayed-job-redis', RedisConfigNode, {
    credentials: {
      password: { type: 'password' },
    },
  });
};
