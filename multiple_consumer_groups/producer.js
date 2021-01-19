const redis = require('redis')
const redisClient = redis.createClient()

const STREAMS_KEY_USER = 'USER' // USER stream
const STREAMS_KEY_LICENSE = 'LICENSE' // LICENSE stream
let userAction = 0
let licenseAction = 0

// produce the message to USER stream
setInterval(() => {
  console.log('\tSending message to USER stream...')
  sendMessage(STREAMS_KEY_USER, userAction)
  userAction++
}, 5000)

// produce the message to LICENSE stream
setInterval(() => {
  console.log('\tSending message to LICENSE stream...')
  sendMessage(STREAMS_KEY_LICENSE, licenseAction)
  licenseAction++
}, 7000)

const sendMessage = (key, action) => {
  redisClient.xadd(key, '*',
    'action', action,
    'data', 'content',
    (err) => { 
      if (err) console.log(err)
    }
  )
}
