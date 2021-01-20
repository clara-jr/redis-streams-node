const async = require('async')
const redis = require('redis')
const redisClient = redis.createClient()

const STREAMS_KEY_USER = 'USER'
const GROUP_ID = 'groupId'
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

const parseMessage = (message) => {
  const id = message[0]
  const values = message[1]
  let messageObject = { id : id }
  for (let i = 0; i < values.length; i = i + 2) {
    messageObject[values[i]] = values[i+1]
  }
  console.log(`Action: ${messageObject.action}`)
  console.log(`Message: ${JSON.stringify(messageObject)}`)
  return { id, messageObject }
}

// claim messages from pending streams
const claimMessage = (streamsKey, next) => {
  redisClient.xpending(streamsKey, GROUP_ID, (err, pendingMessages) => {
    if (err) {
      next(err)
    }
    if (pendingMessages && pendingMessages[0]) {
      console.log(`${pendingMessages[0]} PENDING MESSAGES AT STREAM ${streamsKey}: ${JSON.stringify(pendingMessages)}`)
      redisClient.xclaim(streamsKey, GROUP_ID, CONSUMER_ID, 10, pendingMessages[1], (err, claimMessage) => {
        if (err) {
          next(err)
        }
        if (claimMessage && claimMessage.length) {
          console.log(`Claiming messages from ${streamsKey} stream`)
          // convert the message into a JSON Object
          const { id, messageObject } = parseMessage(claimMessage[0])
          redisClient.xack(streamsKey, GROUP_ID, id, (err) => {
            if (err) {
              console.log(`ACK ERROR: ${err}`)
              next(err)
            }
            console.log(`✅ CLAIM STREAM: ${streamsKey}, ACTION: ${messageObject.action}, ID: ${id}`)
            readMessage(STREAMS_KEY_USER, next)
          })
        } else {
          console.log(`Unable to claim ${pendingMessages[1]}`)
          readMessage(STREAMS_KEY_USER, next)
        }
      })
    }
    else {
      console.log('No pending')
      readMessage(STREAMS_KEY_USER, next)
    }
  })
}

// read messages from streams
const readMessage = (streamsKey, next) => {
  redisClient.xreadgroup('GROUP', GROUP_ID, CONSUMER_ID, 'BLOCK', 1000, 'COUNT', 10, 'STREAMS', streamsKey, '>', (err, readingMessages) => {
    if (err) {
      next(err)
    }
    if (readingMessages) {
      console.log(`Reading messages from ${streamsKey} stream`)
      const messages = readingMessages[0][1]
      // print all messages
      for (message of messages) {
        // convert the message into a JSON Object
        const { id, messageObject } = parseMessage(message)
        redisClient.xack(streamsKey, GROUP_ID, id, (err) => {
          if (err) {
            console.log(`ACK ERROR: ${err}`)
            next(err)
          }
          console.log(`✅ READ STREAM: ${streamsKey}, ACTION: ${messageObject.action}, ID: ${id}`)
          next()
        })
      }
    }
    else {
      console.log('No buffering')
      next()
    }
  })
}

async.forever(
  (next) => {
    claimMessage(STREAMS_KEY_USER, next)
  },
  (err) => {
    console.log(`ERROR: ${err}`)
  }
)
