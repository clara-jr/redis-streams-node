# Event-Driven Architecture with Redis Streams + Node.js

This project shows how to use [Redis Node client](http://redis.js.org/) to publish and consume messages using consumer groups subscribed to multiple streams.

This example represents two test-environments:

- one_consumer_group: single consumer group subscribed to multiple streams

- multiple_consumer_groups: multiple consumer groups subscribed to multiple shared streams.

## Build

```bash
> cd redis-streams-node

> npm install
```

## Run

Make sure you have access to a Redis instance.

Open N terminals.

### Run N-1 Consumers

```bash
> cd one_consumer_group

> node consumer.js 
```

### Run the Producer

```bash
> cd one_consumer_group

> node producer.js
```
