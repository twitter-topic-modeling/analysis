const feed = require('./feed')
const { rssList } = require('./config')

const feeder = feed.init(rssList)
