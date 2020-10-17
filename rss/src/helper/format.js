const _ = require('lodash')

const formatNews = (source, news) => {
  const image = _.get(news, 'enclosures[0].url')
  const extractedImage = _.omit(news, 'enclosures')

  const safe = _.pick(news, ['title', 'summary', 'pubdate', 'link'])
  return { ...safe, source, image }
}

module.exports = formatNews
