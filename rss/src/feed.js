const formatNews = require('./helper/format')

const ora = require('ora')
const RssFeedEmitter = require('rss-feed-emitter')
const kafka = require('./kafka')

const feeder = new RssFeedEmitter({ skipFirstLoad: false })
module.exports = {
  init: (rssList, cb) => {
    console.log('⌛ Initial rss list')
    for (const rss of rssList) {
      const spinner = ora(`Add "${rss.eventName}" rss feed`).start()
      // add new sourse
      feeder.add(rss)
      feeder.on(rss.eventName, async function (item) {
        try {
          const feed = formatNews(rss.eventName, item)
          kafka.send(feed)
        } catch (error) {
          console.log(error)
        }
      })
      spinner.succeed()
    }
    return feeder
  },
}