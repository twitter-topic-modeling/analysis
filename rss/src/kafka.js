const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092'],
})

const producer = kafka.producer()
producer.connect()

const topic = 'rss-test-1'

module.exports = {
  send: async (item) => {
    // await producer.connect()
    const payload = { topic, messages: [{ value: JSON.stringify(item) }] }
    await producer.send(payload)
  },
}
