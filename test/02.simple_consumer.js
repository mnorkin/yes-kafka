'use strict';

/* global describe, it, before, sinon, after  */

var Kafka   = require('../lib/index');
var _       = require('lodash');

var kafkaTestkit = require('./testkit/kafka');

describe('SimpleConsumer', function () {
  describe('Single topic', function () {
    var producer;
    var consumer;

    var dataHandlerSpy = sinon.spy(async (messageSet, topic, partition) => {
      await Promise.all(messageSet.map(async (m) => {
        return await consumer.commitOffset({ topic: topic, partition: partition, offset: m.offset, })
      }))
    });

    before(function () {
      producer = new Kafka.Producer({
        requiredAcks: 1,
        clientId: 'producer2',
      });
      consumer = new Kafka.SimpleConsumer({
        idleTimeout: 100,
        clientId: 'simple-consumer',
      });
      return Promise.all([
        producer.init(),
        consumer.init(),
      ])
        .then(function () {
          return kafkaTestkit.createTopics([
            'kafka-simple-consumer-topic-1',
            'kafka-simple-consumer-topic-2',
            'kafka-simple-consumer-topic-3',
          ]);
        })
        .then(function () {
          return consumer.subscribe('kafka-simple-consumer-topic-1', 0, dataHandlerSpy);
        });
    });

    after(function () {
      return Promise.all([
        consumer.end(),
        producer.end(),
      ]);
    });

    afterEach(function () {
      dataHandlerSpy.reset();
    });

    it('required methods', function () {
      return consumer.should
        .respondTo('init')
        .respondTo('subscribe')
        .respondTo('offset')
        .respondTo('unsubscribe')
        .respondTo('commitOffset')
        .respondTo('fetchOffset')
        .respondTo('end');
    });

    it('should receive new messages', function () {
      return producer.send({
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        message: { value: 'p00', },
      })
        .then(() => new Promise(resolve => setTimeout(resolve, 500)))
        .then(function () {
          /* jshint expr: true */
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
          dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.lastCall.args[1].should.be.a('string');
          dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-simple-consumer-topic-1');
          dataHandlerSpy.lastCall.args[2].should.be.a('number');
          dataHandlerSpy.lastCall.args[2].should.be.eql(0);
          dataHandlerSpy.lastCall.args[3].should.be.a('number');

          dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
          dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
          dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
          dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });
    it('should receive new keyed messages', function () {
      return producer.send({
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        message: {
          key: 'test-key-p00',
          value: 'p00',
        },
      })
        .then(() => new Promise(resolve => setTimeout(resolve, 500)))
        .then(function () {
          /* jshint expr: true */
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
          dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.lastCall.args[1].should.be.a('string');
          dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-simple-consumer-topic-1');
          dataHandlerSpy.lastCall.args[2].should.be.a('number');
          dataHandlerSpy.lastCall.args[2].should.be.eql(0);
          dataHandlerSpy.lastCall.args[3].should.be.a('number');

          dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
          dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
          dataHandlerSpy.lastCall.args[0][0].message.should.have.property('key');
          dataHandlerSpy.lastCall.args[0][0].message.key.toString().should.be.eql('test-key-p00');
          dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
          dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p00');
        });
    });

    it('should correctly encode/decode utf8 string message value', function () {
      return producer.send({
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        message: { value: '人人生而自由，在尊嚴和權利上一律平等。', },
      })
        .then(() => new Promise(resolve => setTimeout(resolve, 500)))
        .then(function () {
          /* jshint expr: true */
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
          dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.lastCall.args[1].should.be.a('string');
          dataHandlerSpy.lastCall.args[1].should.be.eql('kafka-simple-consumer-topic-1');
          dataHandlerSpy.lastCall.args[2].should.be.a('number');
          dataHandlerSpy.lastCall.args[2].should.be.eql(0);

          dataHandlerSpy.lastCall.args[0][0].should.be.an('object');
          dataHandlerSpy.lastCall.args[0][0].should.have.property('message').that.is.an('object');
          dataHandlerSpy.lastCall.args[0][0].message.should.have.property('value');
          dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('人人生而自由，在尊嚴和權利上一律平等。');
        });
    });

    it('offset() should return last offset', function () {
      return consumer.offset('kafka-simple-consumer-topic-1', 0).then(function (offset) {
        offset.should.be.a('number').and.be.gt(0);
      });
    });

    it('should reset offset to LATEST on OffsetOutOfRange error', function () {
      return consumer.offset('kafka-simple-consumer-topic-1', 0).then(function (offset) {
        return consumer.subscribe('kafka-simple-consumer-topic-1', 0, { offset: offset + 200, }, dataHandlerSpy)
          .then(function () {
            consumer.subscriptions['kafka-simple-consumer-topic-1:0'].offset.should.be.eql(offset + 200);
          })
          .then(() => new Promise(resolve => setTimeout(resolve, 300)))
          .then(function () {
            consumer.subscriptions['kafka-simple-consumer-topic-1:0'].offset.should.be.eql(offset);
          });
      });
    });

    it('should receive messages from specified offset', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1', 0).then(function () {
        return producer.send([{
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
          message: { value: 'p000', },
        }, {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
          message: { value: 'p001', },
        },]);
      })
        .then(function () {
          return consumer.offset('kafka-simple-consumer-topic-1', 0).then(function (offset) {
            return consumer.subscribe('kafka-simple-consumer-topic-1', 0, { offset: offset - 2, }, dataHandlerSpy)
              .then(() => new Promise(resolve => setTimeout(resolve, 200))) // consumer sleep timeout
              .then(function () {
                        dataHandlerSpy.should.have.been.called; // eslint-disable-line
                dataHandlerSpy.lastCall.args[0].should.be.an('array').and.have.length(2);
                dataHandlerSpy.lastCall.args[0][0].message.value.toString('utf8').should.be.eql('p000');
                dataHandlerSpy.lastCall.args[0][1].message.value.toString('utf8').should.be.eql('p001');
              });
          });
        });
    });

    it('should receive messages in maxBytes batches', function () {
      var maxBytesTestMessagesSize = 1024;
      return consumer.unsubscribe('kafka-simple-consumer-topic-1', 0).then(function () {
        return consumer.offset('kafka-simple-consumer-topic-1', 0).then(function (offset) {
          // ask for maxBytes that is only 1 byte less then required for both last messages
          var maxBytes = 2 * (8 + 4) + maxBytesTestMessagesSize - 1;
          return consumer.subscribe('kafka-simple-consumer-topic-1', 0, { offset: offset - 2, maxBytes: maxBytes, }, dataHandlerSpy)
            .then(() => new Promise(resolve => setTimeout(resolve, 300)))
            .then(function () {
              /* jshint expr: true */
                        dataHandlerSpy.should.have.been.called; // eslint-disable-line
              dataHandlerSpy.getCall(0).args[0].should.be.an('array').and.have.length(2);
              dataHandlerSpy.getCall(0).args[0][0].message.value.toString('utf8').should.be.eql('p000');
              dataHandlerSpy.getCall(0).args[0][1].message.value.toString('utf8').should.be.eql('p001');
            });
        });
      });
    });

    // this is confusing test
    it.skip('should skip single message larger then configured maxBytes', function () {
      var mSize;
      return producer.send([{
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        message: { value: 'p0000000000000001', },
      }, {
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        message: { value: 'p001', },
      },])
        .then(() => new Promise(resolve => setTimeout(resolve, 300)))
        .then(function () {
                dataHandlerSpy.should.have.been.called; // eslint-disable-line
          mSize = dataHandlerSpy.getCall(0).args[0][0].messageSize;
        })
        .then(function () {
          dataHandlerSpy.reset();
          return consumer.unsubscribe('kafka-simple-consumer-topic-1', 0).then(function () {
            return consumer.offset('kafka-simple-consumer-topic-1', 0).then(function (offset) {
              // ask for maxBytes that is smaller then size of the first message but enough to receive second message
              var maxBytes = 8 + 4 + mSize - 1;
              return consumer.subscribe('kafka-simple-consumer-topic-1', 0, { offset: offset - 2, maxBytes: maxBytes, }, dataHandlerSpy)
                .then(() => new Promise(resolve => setTimeout(resolve, 300)))
                .then(function () {
                            dataHandlerSpy.should.have.been.calledTwice; // eslint-disable-line
                  dataHandlerSpy.getCall(0).args[0].should.be.an('array').and.have.length(1);
                  dataHandlerSpy.getCall(0).args[0][0].message.value.toString('utf8').should.be.eql('p001');
                });
            });
          });
        });
    });

    it('should be able to commit single offset', function () {
      return consumer.commitOffset({
        topic: 'kafka-simple-consumer-topic-1',
        partition: 0,
        offset: 1,
        metadata: 'm1',
      })
        .then(function (result) {
          result.should.be.an('array').that.has.length(1);
          result[0].should.be.an('object');
          result[0].should.have.property('topic', 'kafka-simple-consumer-topic-1');
          result[0].should.have.property('partition').that.is.a('number');
          result[0].should.have.property('error', null);
        });
    });

    it('should be able to commit offsets', function () {
      return consumer.commitOffset([
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
          offset: 1,
          metadata: 'm1',
        },
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 1,
          offset: 2,
          metadata: 'm2',
        },
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 2,
          offset: 3,
          metadata: 'm3',
        },
      ]).then(function (result) {
        result.should.be.an('array').that.has.length(3);
        result[0].should.be.an('object');
        result[1].should.be.an('object');
        result[2].should.be.an('object');
        result[0].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[1].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[2].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[0].should.have.property('partition').that.is.a('number');
        result[1].should.have.property('partition').that.is.a('number');
        result[2].should.have.property('partition').that.is.a('number');
        result[0].should.have.property('error', null);
        result[1].should.have.property('error', null);
        result[2].should.have.property('error', null);
      });
    });

    it('should be able to fetch commited offsets', function () {
      return consumer.fetchOffset([
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
        },
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 1,
        },
        {
          topic: 'kafka-simple-consumer-topic-1',
          partition: 2,
        },
      ]).then(function (result) {
        result.should.be.an('array').that.has.length(3);
        result[0].should.be.an('object');
        result[1].should.be.an('object');
        result[2].should.be.an('object');
        result[0].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[1].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[2].should.have.property('topic', 'kafka-simple-consumer-topic-1');
        result[0].should.have.property('partition').that.is.a('number');
        result[1].should.have.property('partition').that.is.a('number');
        result[2].should.have.property('partition').that.is.a('number');
        result[0].should.have.property('offset').that.is.a('number');
        result[1].should.have.property('offset').that.is.a('number');
        result[2].should.have.property('offset').that.is.a('number');
        result[0].should.have.property('error', null);
        result[1].should.have.property('error', null);
        result[2].should.have.property('error', null);
        _.find(result, { topic: 'kafka-simple-consumer-topic-1', partition: 0, }).offset.should.be.eql(1);
        _.find(result, { topic: 'kafka-simple-consumer-topic-1', partition: 1, }).offset.should.be.eql(2);
        _.find(result, { topic: 'kafka-simple-consumer-topic-1', partition: 2, }).offset.should.be.eql(3);
      });
    });

    it('should unsubscribe all partitions in a topic when partition is not specified', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1').then(function () {
        consumer.subscriptions.should.not.have.property('kafka-simple-consumer-topic-1:0');
        consumer.subscriptions.should.not.have.property('kafka-simple-consumer-topic-1:1');
        consumer.subscriptions.should.not.have.property('kafka-simple-consumer-topic-1:2');
      });
    });

    it('should subscribe all partitions in a topic when partition is not specified', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1').then(function () {
        return consumer.subscribe('kafka-simple-consumer-topic-1', dataHandlerSpy).then(function () {
          consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:0');
          consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:1');
          consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:2');
        });
      });
    });

    it('should subscribe partitions specified as array', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1').then(function () {
        return consumer.subscribe('kafka-simple-consumer-topic-1', [0, 1,], dataHandlerSpy).then(function () {
          consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:0');
          consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:1');
          consumer.subscriptions.should.not.have.property('kafka-simple-consumer-topic-1:2');
        });
      });
    });

    it('should subscribe partitions specified as array when options specified', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1').then(function () {
        return consumer.subscribe('kafka-simple-consumer-topic-1', [0, 1,], {}, dataHandlerSpy)
          .then(function () {
            consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:0');
            consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:1');
            consumer.subscriptions.should.not.have.property('kafka-simple-consumer-topic-1:2');
          });
      });
    });

    it('should subscribe all topic partitions when partition is not specified but options specified', function () {
      return consumer.unsubscribe('kafka-simple-consumer-topic-1').then(function () {
        return consumer.subscribe('kafka-simple-consumer-topic-1', {}, dataHandlerSpy)
          .then(function () {
            consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:0');
            consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:1');
            consumer.subscriptions.should.have.property('kafka-simple-consumer-topic-1:2');
          });
      });
    });

    it('should throw when missing dataHandler function', function () {
      return consumer.subscribe('kafka-simple-consumer-topic-1', [0, 1,], {}).should.be.rejected;
    });

    it('should ignore sync errors in data handler', function () {
      var spy = sinon.spy(function () {
        throw new Error();
      });
      return consumer.subscribe('kafka-simple-consumer-topic-1', 0, spy).then(function () {
        return producer.send({
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
          message: { value: 'p00', },
        });
      })
        .then(() => new Promise(resolve => setTimeout(resolve, 200)))
        .then(function () {
                spy.should.have.been.called; // eslint-disable-line
        });
    });

    it('should ignore async errors in data handler', function () {
      var spy = sinon.spy(function () {
        return Promise.reject(new Error());
      });
      return consumer.subscribe('kafka-simple-consumer-topic-1', 0, spy).then(function () {
        return producer.send({
          topic: 'kafka-simple-consumer-topic-1',
          partition: 0,
          message: { value: 'p00', },
        });
      })
        .then(() => new Promise(resolve => setTimeout(resolve, 200)))
        .then(function () {
                spy.should.have.been.called; // eslint-disable-line
        });
    });
  });

  describe('Multiple topic', function () {
    var producer;
    var consumer;
    var dataHandlerSpy = sinon.spy(function () {});

    before(function () {
      producer = new Kafka.Producer({
        requiredAcks: 1,
        clientId: 'producer',
      });
      consumer = new Kafka.SimpleConsumer({
        idleTimeout: 100,
        clientId: 'simple-consumer',
      });
      return Promise.all([
        producer.init(),
        consumer.init(),
      ]);
    });

    after(function () {
      return Promise.all([
        consumer.end(),
        producer.end(),
      ]);
    });

    afterEach(function () {
      dataHandlerSpy.reset();
    });

    it('should receive messages from multiple topics', function () {
      return Promise.all([
        consumer.subscribe('kafka-simple-consumer-topic-1', 0, dataHandlerSpy).then(function () {
          return producer.send({
            topic: 'kafka-simple-consumer-topic-1',
            partition: 0,
            message: { value: 'p00', },
          });
        }),
        consumer.subscribe('kafka-simple-consumer-topic-2', 0, dataHandlerSpy).then(function () {
          return producer.send({
            topic: 'kafka-simple-consumer-topic-2',
            partition: 0,
            message: { value: 'p01', },
          });
        }),
        consumer.subscribe('kafka-simple-consumer-topic-3', 0, dataHandlerSpy).then(function () {
          return producer.send({
            topic: 'kafka-simple-consumer-topic-3',
            partition: 0,
            message: { value: 'p01', },
          });
        }),
      ])
        .then(() => new Promise(resolve => setTimeout(resolve, 500)))
        .then(function () {
          var topics = [
            dataHandlerSpy.getCall(0).args[1],
            dataHandlerSpy.getCall(1).args[1],
            dataHandlerSpy.getCall(2).args[1],
          ];
                dataHandlerSpy.should.have.been.calledThrice; // eslint-disable-line

          topics.should.include('kafka-simple-consumer-topic-1');
          topics.should.include('kafka-simple-consumer-topic-2');
          topics.should.include('kafka-simple-consumer-topic-3');

          dataHandlerSpy.getCall(0).args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.getCall(0).args[1].should.be.a('string');
          dataHandlerSpy.getCall(0).args[2].should.be.a('number');
          dataHandlerSpy.getCall(0).args[2].should.be.eql(0);
          dataHandlerSpy.getCall(0).args[3].should.be.a('number');

          dataHandlerSpy.getCall(1).args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.getCall(1).args[1].should.be.a('string');
          dataHandlerSpy.getCall(1).args[2].should.be.a('number');
          dataHandlerSpy.getCall(1).args[2].should.be.eql(0);
          dataHandlerSpy.getCall(1).args[3].should.be.a('number');

          dataHandlerSpy.getCall(2).args[0].should.be.an('array').and.have.length(1);
          dataHandlerSpy.getCall(2).args[1].should.be.a('string');
          dataHandlerSpy.getCall(2).args[2].should.be.a('number');
          dataHandlerSpy.getCall(2).args[2].should.be.eql(0);
          dataHandlerSpy.getCall(2).args[3].should.be.a('number');
        });
    });
  });
});
