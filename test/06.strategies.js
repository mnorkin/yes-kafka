'use strict';

/* global describe, it, before, after  */

var Kafka   = require('../lib/index');
var _       = require('lodash');
var kafkaTestkit = require('./testkit/kafka');

var admin;

before(async () => {
  admin = new Kafka.GroupAdmin({
    clientId: 'admin',
  });
  await admin.init();
});

after(async () => {
  await admin.end();
});

describe('Weighted Round Robin Assignment', () => {
  var KAFKA_TOPIC = 'kafka-strategies-topic';
  var consumers = [
    new Kafka.GroupConsumer({
      groupId: 'no-kafka-group-v0.9-wrr',
      idleTimeout: 100,
      heartbeatTimeout: 100,
      clientId: 'group-consumer1',
    }),
    new Kafka.GroupConsumer({
      groupId: 'no-kafka-group-v0.9-wrr',
      idleTimeout: 100,
      heartbeatTimeout: 100,
      clientId: 'group-consumer2',
    }),
  ];

  before(async () => {
    await kafkaTestkit.createTopics([KAFKA_TOPIC,])

    await Promise.all(consumers.map(async (consumer, ind) => {
      await consumer.init({
        subscriptions: [KAFKA_TOPIC,],
        metadata: {
          weight: ind + 1,
        },
        strategy: new Kafka.WeightedRoundRobinAssignmentStrategy(),
        handler: function () {},
      });
    }));

    await new Promise(resolve => setTimeout(resolve, 200));
  });

  after(async () => {
    await Promise.all(consumers.map(async (c) => await c.end() ));
  });

  it('should split partitions according to consumer weight', async () => {
    const group = await admin.describeGroup(consumers[0].options.groupId);

    var consumer1 = _.find(group.members, { clientId: 'group-consumer1', });
    var consumer2 = _.find(group.members, { clientId: 'group-consumer2', });

    consumer1.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(1);
    consumer2.memberAssignment.partitionAssignment[0].partitions.should.be.an('array').and.have.length(2);
  });
});

describe('Consistent Assignment', () => {
  var KAFKA_TOPIC = 'kafka-consistent-assignment-test';
  var consumers = [
    new Kafka.GroupConsumer({
      groupId: 'no-kafka-group-v0.9-ring',
      idleTimeout: 100,
      heartbeatTimeout: 100,
      clientId: 'group-consumer1',
    }),
    new Kafka.GroupConsumer({
      groupId: 'no-kafka-group-v0.9-ring',
      idleTimeout: 100,
      heartbeatTimeout: 100,
      clientId: 'group-consumer2',
    }),
  ];

  before(async () => {
    await kafkaTestkit.createTopics([KAFKA_TOPIC,]);

    await Promise.all(consumers.map(async (consumer, ind,) => {
      await consumer.init({
        subscriptions: [KAFKA_TOPIC,],
        metadata: {
          id: 'id' + ind,
          weight: 10,
        },
        strategy: new Kafka.ConsistentAssignmentStrategy(),
        handler: function () {},
      });
    }));

    await new Promise(resolve => setTimeout(resolve, 200));
  });

  after(async () => {
    await Promise.all(consumers.map(async (c) => await c.end()));
  });

  it('should split partitions according to consumer weight', async () => {
    const group = await admin.describeGroup(consumers[0].options.groupId);

    var consumer1 = _.find(group.members, { clientId: 'group-consumer1', });
    var consumer2 = _.find(group.members, { clientId: 'group-consumer2', });
    var consumer1Partition = consumer1.memberAssignment.partitionAssignment[0].partitions[0];

    consumer1.memberAssignment.partitionAssignment[0].partitions.should.have.length(1);

    consumer2.memberAssignment.partitionAssignment[0].partitions.should.have.length(2);
    consumer2.memberAssignment.partitionAssignment[0].partitions.should.not.include(consumer1Partition);
  });
});
