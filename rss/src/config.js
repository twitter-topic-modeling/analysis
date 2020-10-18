const kafka = require('./kafka')

const REFRESH_RATE = 200

const rssList = [
  {
    url: 'https://www.thairath.co.th/rss/news',
    eventName: 'ไทยรัฐ',
  },
  {
    url: 'http://rssfeeds.sanook.com/rss/feeds/sanook/news.index.xml',
    eventName: 'สนุกดอทคอม',
  },
  { url: 'https://news.thaipbs.or.th/rss/news', eventName: 'thaipbs' },
  { url: 'https://www.prachachat.net/feed', eventName: 'ประชาชาติ' },
  { url: 'https://www.matichon.co.th/feed', eventName: 'มติชน' },
  { url: 'https://voicetv.co.th/rss', eventName: 'Voice TV' },
].map((rss) => ({ ...rss, refresh: REFRESH_RATE }))

module.exports = { rssList }
