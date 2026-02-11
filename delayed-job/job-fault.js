module.exports = function (RED) {
  function JobFaultNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    const errorMessage = config.errorMessage || 'Job failed';

    node.on('input', function (msg, send, done) {
      if (!msg._jobId) {
        node.warn('No _jobId found in message, cannot fault job');
        if (done) done();
        return;
      }

      if (!msg._jobSourceNode) {
        node.warn('No _jobSourceNode found in message, cannot fault job');
        if (done) done();
        return;
      }

      const sourceNode = RED.nodes.getNode(msg._jobSourceNode);
      if (!sourceNode || typeof sourceNode.handleAck !== 'function') {
        node.warn('Source delayed-job node not found: ' + msg._jobSourceNode);
        if (done) done();
        return;
      }

      const errMsg = msg._jobError || errorMessage;
      sourceNode.handleAck(msg._jobId, false, errMsg);

      if (done) done();
    });
  }

  RED.nodes.registerType('job-fault', JobFaultNode);
};
