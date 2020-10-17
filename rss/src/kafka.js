const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  brokers: ['localhost:9092'],
})

const producer = kafka.producer()
const topic = 'rss-test-1'

module.exports = {
  send: async (item) => {
    const payload = { topic, messages: [item] }
    await producer.connect()
    await producer.send(payload)
  },
}
