'use strict';

var _ = require('lodash');
var Client = require('./client');
var Kafka = require('./index');
var errors = require('./errors');

function Producer(options) {
  this.options = _.defaultsDeep(options || {}, {
    requiredAcks: 1,
    timeout: 30000,
    partitioner: new Kafka.DefaultPartitioner(),
    retries: {
      attempts: 3,
      delay: {
        min: 1000,
        max: 3000,
      },
    },
    batch: {
      size: 16384,
      maxWait: 10,
    },
    codec: Kafka.COMPRESSION_NONE,
  });

  if (this.options.partitioner instanceof Kafka.DefaultPartitioner) {
    this.partitioner = this.options.partitioner;
  } else {
    throw new Error('Partitioner must inherit from Kafka.DefaultPartitioner');
  }

  this.client = new Client(this.options);

  this.queue = {};
}

module.exports = Producer;

/**
 * Initialize Producer
 *
 * @return {Promise}
 */
Producer.prototype.init = async function () { await this.client.init() };

Producer.prototype._prepareProduceRequest = async function (data) {
  return await Promise.all(data.map(async (d) => {
    if (typeof d.topic !== 'string' || d.topic === '') {
      throw new Error('Missing or wrong topic field');
    }
    if (typeof d.partition !== 'number' || d.partition < 0) {
      const partitions = await this.client.getTopicPartitions(d.topic);
      try {
        const partition = await this.partitioner.partition(d.topic, partitions, d.message);
        d.partition = partition;
      } catch (e) {
        d.partition = -1;
      }
    }

    const leader = await this.client.findLeader(d.topic, d.partition);
    d.leader = leader;

    return d;
  }));
}

Producer.prototype._send = async function (hash) {
  var self = this, task = self.queue[hash], data, result = [];

  delete self.queue[hash];

  data = Array.prototype.concat.apply([], task.data);

  try {
    await (async function _try(_data, attempt) {
      attempt = attempt || 1;

      const requests = await self._prepareProduceRequest(_data);
      const response = await self.client.produceRequest(requests, task.options.codec);

      var toRetry = [];
      if (_.isEmpty(response)) { // if requiredAcks = 0
        return response;
      }

      await Promise.all(response.map(async (p) => {
        if (p.error) {
          if (
            (/UnknownTopicOrPartition|NotLeaderForPartition|LeaderNotAvailable/.test(p.error.code) || p.error instanceof errors.NoKafkaConnectionError)
            && attempt < task.options.retries.attempts
          ) {
            self.client.debug('Received', p.error, 'for', p.topic + ':' + p.partition);
            toRetry = toRetry.concat(_.filter(_data, { topic: p.topic, partition: p.partition, }));
          } else {
            result.push(p);
          }
        } else {
          result.push(p);
        }
      }));

      if (toRetry.length) {
        const delay = _.min([attempt * task.options.retries.delay.min, task.options.retries.delay.max,]);
        await new Promise(resolve => setTimeout(resolve, delay));
        const topics = toRetry.map((retry) => retry.topic );

        await Promise.all(topics.map(async (topic) => self.client.updateTopicMetadata(topic)));

        await _try(toRetry, ++attempt);
      }
    })(data);

    task.resolve(result);
  } catch (e) {
    task.reject(e);
  }
};

/**
 * Send message or messages to Kafka
 *
 * @param  {Object|Array} data [{ topic, partition, message: {key, value, attributes} }]
 * @param  {Object} options { codec, retries: { attempts, delay: { min, max } }, batch: { size } }
 * @return {Promise}
 */
Producer.prototype.send = function (data, options) {
  var self = this, hash, promise, task;

  if (!Array.isArray(data)) {
    data = [data,];
  }

  options = _.merge({}, {
    codec: self.options.codec,
    retries: self.options.retries,
    batch: self.options.batch,
  }, options || {});

  hash = [
    options.codec,
    options.retries.attempts,
    options.retries.delay.min,
    options.retries.delay.max,
    options.batch.size,
    options.batch.maxWait,
  ].join('.');

  if (self.queue[hash] === undefined) {
    promise = new Promise(function (resolve, reject) {
      self.queue[hash] = {
        timeout: null,
        resolve: resolve,
        reject: reject,
        options: options,
        data: [],
        dataSize: 0,
      };
    });
    self.queue[hash].promise = promise;
  }

  task = self.queue[hash];
  task.data.push(data);
  task.dataSize += _.sumBy(data, _.partialRight(_.get, 'message.value.length', 0));

  if (task.dataSize >= options.batch.size || options.batch.maxWait === 0) {
    if (task.timeout !== null) {
      clearTimeout(task.timeout);
    }
    self._send(hash);
  } else if (task.timeout === null) {
    task.timeout = setTimeout(function () {
      self._send(hash);
    }, options.batch.maxWait);
  }

  return task.promise;
};

Producer.prototype.end = async function () { await this.client.end(); }
