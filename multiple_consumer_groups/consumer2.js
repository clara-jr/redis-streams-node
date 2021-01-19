const async = require('async')
const redis = require('redis')
const redisClient = redis.createClient()

const STREAMS_KEY_USER = 'USER'
const STREAMS_KEY_LICENSE = 'LICENSE'
const GROUP_ID = 'groupId2'
const CONSUMER_ID = 'consumerId'

// create one group per stream you want to receive messages from
const createGroup = (streamsKey) => {
  redisClient.xgroup('CREATE', streamsKey, GROUP_ID, '$', 'MKSTREAM', (err) => {
    if (err) {
      if (err.code == 'BUSYGROUP' ) {
        console.log(`Group ${GROUP_ID} already exists at stream ${streamsKey}`)
      } else {
        console.log(err)
      }
    }
  })
}
createGroup(STREAMS_KEY_USER)
createGroup(STREAMS_KEY_LICENSE)

// read messages from streams
const readMessage = (streamsKey, next) => {
  redisClient.xreadgroup('GROUP', GROUP_ID,  CONSUMER_ID , 'BLOCK', 1000, 'COUNT', 10, 'NOACK', 'STREAMS', streamsKey, '>', (err, stream) => {
    if (err) {
      next(err)
    }
    if (stream) {
      console.log(`Reading messages from ${streamsKey} stream`)
      const messages = stream[0][1]
      // print all messages
      messages.forEach((message) => {
        // convert the message into a JSON Object
        const id = message[0]
        const values = message[1]
        let messageObject = { id : id }
        for (let i = 0; i < values.length; i = i + 2) {
          messageObject[values[i]] = values[i+1]
        }
        console.log(`Action: ${messageObject.action}`)
        console.log(`Message: ${JSON.stringify(messageObject)}`)
      })
    }
    next()
  })
}
async.forever(
  (next) => {
    readMessage(STREAMS_KEY_USER, next)
    readMessage(STREAMS_KEY_LICENSE, next)
  },
  (err) => {
    console.log(`ERROR: ${err}`)
  }
)
