const _ = require('lodash')

const formatNews = (source, news) => {
  const image = _.get(news, 'enclosures[0].url')
  const extractedImage = _.omit(news, 'enclosures')
  const safe = _.pick(news, ['title', 'summary', 'pubDate'])
  const url = news['link']
  const pubDate = _.get(news, 'meta.pubDate', new Date()).toISOString()
  return { ...safe, pubDate, url, source }
}

module.exports = formatNews
