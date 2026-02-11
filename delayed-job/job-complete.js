module.exports = function (RED) {
  function JobCompleteNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    node.on('input', function (msg, send, done) {
      if (!msg._jobId) {
        node.warn('No _jobId found in message, cannot complete job');
        if (done) done();
        return;
      }

      if (!msg._jobSourceNode) {
        node.warn('No _jobSourceNode found in message, cannot complete job');
        if (done) done();
        return;
      }

      const sourceNode = RED.nodes.getNode(msg._jobSourceNode);
      if (!sourceNode || typeof sourceNode.handleAck !== 'function') {
        node.warn('Source delayed-job node not found: ' + msg._jobSourceNode);
        if (done) done();
        return;
      }

      sourceNode.handleAck(msg._jobId, true);

      if (done) done();
    });
  }

  RED.nodes.registerType('job-complete', JobCompleteNode);
};
