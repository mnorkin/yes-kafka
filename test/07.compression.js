'use strict';

/* global describe, it, before, sinon, after  */

var crc32   = require('buffer-crc32');
var Kafka   = require('../lib/index');
var kafkaTestkit = require('./testkit/kafka');

describe('Compression', function () {
  describe('sync', function () {
    var KAFKA_TOPIC = 'kafka-compression-sync-test-topic';
    var producer;
    var consumer;

    var dataHandlerSpy = sinon.spy(function () {});

    before(async () => {
      producer = new Kafka.Producer({
        clientId: 'producer',
        asyncCompression: false,
      });

      consumer = new Kafka.SimpleConsumer({
        idleTimeout: 100,
        clientId: 'simple-consumer',
        asyncCompression: false,
      });
      await kafkaTestkit.createTopics([KAFKA_TOPIC,]);
      await producer.init();
      await consumer.init();

      await consumer.subscribe(KAFKA_TOPIC, 0, dataHandlerSpy);
    });

    after(async () => {
      await producer.end();
      await consumer.end();
    });

    it('should send/receive with Snappy compression (<32kb)', async () => {
      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p00', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p01', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await new Promise(resolve => setTimeout(resolve, 20));

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p02', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      const _offset = await consumer.offset(KAFKA_TOPIC, 0, Kafka.LATEST_OFFSET);
      const offset = _offset - 3;

      await consumer.subscribe(KAFKA_TOPIC, 0, { offset: offset, }, dataHandlerSpy);

      await new Promise(resolve => setTimeout(resolve, 300));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(3);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
      dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p01');
      dataHandlerSpy.lastCall.args[0][2].message.value.toString('utf8').should.be.eql('p02');
    });

    it('should send/receive with Snappy compression (>32kb)', async () => {
      const buf = new Buffer(90 * 1024);
      const crc = crc32.signed(buf);

      dataHandlerSpy.reset();

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: buf, },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await new Promise(resolve => setTimeout(resolve, 300));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
    });

    if (typeof require('zlib').gzipSync === 'function') {
      it('should send/receive with gzip compression', async () => {
        dataHandlerSpy.reset();
        await producer.send({
          topic: KAFKA_TOPIC,
          partition: 0,
          message: { value: 'p00', },
        }, { codec: Kafka.COMPRESSION_GZIP, });

        await new Promise(resolve => setTimeout(resolve, 200));

        dataHandlerSpy.should.have.been.called; // eslint-disable-line
        dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
        dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
        dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

        dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
        dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
        dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
        dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
      });
    }

    it('producer should send uncompressed message when codec is not supported', async () => {
      dataHandlerSpy.reset();

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p00', },
      }, { codec: 30, });

      await new Promise(resolve => setTimeout(resolve, 200));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
    });
  });

  describe('async', function () {
    var KAFKA_TOPIC = 'kafka-compression-async-test-topic';
    var producer;
    var consumer;

    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
      producer = new Kafka.Producer({
        clientId: 'producer',
        asyncCompression: true,
      });
      consumer = new Kafka.SimpleConsumer({
        idleTimeout: 100,
        clientId: 'simple-consumer',
        asyncCompression: true,
      });

      return kafkaTestkit.createTopics([
        KAFKA_TOPIC,
      ]).then(function () {
        return Promise.all([
          producer.init(),
          consumer.init(),
        ]);
      })
      .then(function () {
        return consumer.subscribe(KAFKA_TOPIC, 0, dataHandlerSpy);
      });
    });

    after(async () => {
      await producer.end();
      await consumer.end();
    });

    it('should send/receive with async Snappy compression (<32kb)', async () => {

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p00', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p01', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await new Promise(resolve => setTimeout(resolve, 20));

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p02', },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      const _offset = await consumer.offset(KAFKA_TOPIC, 0, Kafka.LATEST_OFFSET);
      const offset = _offset - 3;

      dataHandlerSpy.reset();

      await consumer.subscribe(KAFKA_TOPIC, 0, { offset: offset, }, dataHandlerSpy);

      await new Promise(resolve => setTimeout(resolve, 200));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(3);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
      dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p01');
      dataHandlerSpy.lastCall.args[0][2].message.value.toString('utf8').should.be.eql('p02');
    });

    it('should send/receive with async Snappy compression (>32kb)', async () => {
      const buf = new Buffer(90 * 1024);
      const crc = crc32.signed(buf);

      dataHandlerSpy.reset();

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: buf, },
      }, { codec: Kafka.COMPRESSION_SNAPPY, });

      await new Promise(resolve => setTimeout(resolve, 300));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
    });

    it('should send/receive with async gzip compression', async () => {
      dataHandlerSpy.reset();

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p00', },
      }, { codec: Kafka.COMPRESSION_GZIP, });

      await new Promise(resolve => setTimeout(resolve, 200));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
    });

    it('should send/receive with async Gzip compression (>32kb)', async () => {
      const buf = new Buffer(90 * 1024);
      const crc = crc32.signed(buf);

      dataHandlerSpy.reset();

      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: buf, },
      }, { codec: Kafka.COMPRESSION_GZIP, });

      await new Promise(resolve => setTimeout(resolve, 300));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      crc32.signed(dataHandlerSpy.lastCall.args[0][0].message.value).should.be.eql(crc);
    });

    it('producer should send uncompressed message when codec is not supported', async () => {
      dataHandlerSpy.reset();
      await producer.send({
        topic: KAFKA_TOPIC,
        partition: 0,
        message: { value: 'p00', },
      }, { codec: 30, });

      await new Promise(resolve => setTimeout(resolve, 200));

      dataHandlerSpy.should.have.been.called; // eslint-disable-line
      dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
      dataHandlerSpy.lastCall.args[1].should.be.a('string', KAFKA_TOPIC);
      dataHandlerSpy.lastCall.args[2].should.be.a('number', 0);

      dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
      dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
      dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
      dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
    });
  });
});
