const feed = require('./feed')
const { rssList } = require('./config')
const kafka = require('./kafka')

// const send = async () => {
//   await kafka.send({
//     summary:
//       '<p>ปกติแล้วในทุก ๆ ปี เมื่อถึงเดือนตุลาคมจะเป็นช่วงเวลาของ [&#8230;]</p>\n<p>อ่านข่าวต้นฉบับ: <a rel="nofollow" href="https://www.prachachat.net/advertorial/news-538914">ทานเจอย่างไรให้ปลอดภัย ได้สารอาหารครบ</a></p>',
//     source: 'ประชาชาติ',
//     title: 'ทานเจอย่างไรให้ปลอดภัย ได้สารอาหารครบ',
//     pubDate: '2020-10-21T01:00:43Z',
//     url: 'https://www.prachachat.net/advertorial/news-538914',
//   })
//   console.log('success')
// }
// send()
const feeder = feed.init(rssList)
