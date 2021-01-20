const redis = require('redis')
const redisClient = redis.createClient()

const STREAMS_KEY_USER = 'USER' // USER stream
let userAction = 0

// produce the message to USER stream
setInterval(() => {
  console.log('\tSending message to USER stream...')
  sendMessage(STREAMS_KEY_USER, userAction)
  userAction++
}, 10000)

const sendMessage = (key, action) => {
  redisClient.xadd(key, '*',
    'action', action,
    'data', 'content',
    (err) => { 
      if (err) console.log(err)
    }
  )
}
