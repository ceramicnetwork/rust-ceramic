import {
  CreateTableInput,
  DescribeTableInput,
  DynamoDB,
  ScanCommandInput,
} from '@aws-sdk/client-dynamodb'
import { StreamID } from '@ceramicnetwork/streamid'
import { DateTime, Duration } from 'luxon'

import { utilities } from './common.js'
const delay = utilities.delay

const Region = process.env.REGION || process.env.AWS_REGION || 'us-east-1'
const DbEndpoint = process.env.DB_ENDPOINT
  ? process.env.DB_ENDPOINT
  : `https://dynamodb.${Region}.amazonaws.com`
const Stage = process.env.STAGE || 'dev'
const TestTable = `ceramic-tests-${Stage}`

export const AnchorInterval = Duration.fromObject({
  minutes: Number(process.env.ANCHOR_INTERVAL_MIN) || 720,
}) // 12 hour default

export const DynamoClient = new DynamoDB({
  region: Region,
  endpoint: DbEndpoint,
})

export const cleanup = async () => {
  DynamoClient.destroy()
}

const DEFAULT_STREAM_TTL_MINS = 60 * 24 * 7 // 1 week
export const StreamTTL = Duration.fromObject({
  minutes: Number(process.env.STREAM_TTL_MIN || DEFAULT_STREAM_TTL_MINS),
})
// Usually streams will be cleaned up by the garbage collection test process. But if something slips
// through the cracks, we set a TTL at the database layer of 2x the expected TTL
const DatabaseTTL = StreamTTL.plus(StreamTTL)

// This function will block until the table is ready
export const createTestTable = async () => {
  const describeIn: DescribeTableInput = {
    TableName: TestTable,
  }
  while (1) {
    try {
      const describeTableOut = await DynamoClient.describeTable(describeIn)
      if (describeTableOut.Table?.TableStatus === 'ACTIVE') {
        console.log('Test table ready')
        return
      }
      console.log('Test table not ready...')
      await delay(5)
      continue
    } catch (e) {
      console.error('Test table does not exist')
    }
    try {
      console.log('Creating test table...')
      const createTableIn: CreateTableInput = {
        AttributeDefinitions: [
          {
            AttributeName: 'StreamId',
            AttributeType: 'S',
          },
        ],
        KeySchema: [
          {
            AttributeName: 'StreamId',
            KeyType: 'HASH',
          },
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 1,
          WriteCapacityUnits: 1,
        },
        TableName: TestTable,
      }
      await DynamoClient.createTable(createTableIn)
    } catch (e) {
      console.error('Could not create test table')
    }
    await delay(5)
  }
}

export const storeStreamReq = async (streamId: StreamID) => {
  try {
    const now = DateTime.utc()
    await DynamoClient.putItem({
      TableName: TestTable,
      Item: {
        StreamId: {
          S: streamId.toString(),
        },
        Creation: {
          N: now.toSeconds().toString(),
        },
        Expiration: {
          N: now.plus(DatabaseTTL).toSeconds().toString(),
        },
        Anchored: {
          BOOL: false,
        },
      },
    })
  } catch (err) {
    throw 'unexpected error: ' + err
  }
}

export const fetchUnanchoredStreamReqs = async (pageSize: number | null = null) => {
  const args: ScanCommandInput = {
    TableName: TestTable,
    FilterExpression: 'Anchored = :anchored',
    ExpressionAttributeValues: { ':anchored': { BOOL: false } },
  }
  return dynamoScan(args, pageSize)
}

export const fetchAnchoredStreamReqs = async (pageSize: number | null = null) => {
  const args: ScanCommandInput = {
    TableName: TestTable,
    FilterExpression: 'Anchored = :anchored',
    ExpressionAttributeValues: { ':anchored': { BOOL: true } },
  }
  return dynamoScan(args, pageSize)
}

export const fetchExpiredStreamReqs = async (pageSize: number | null = null) => {
  const now = DateTime.utc()
  const expired = now.minus(StreamTTL).toSeconds().toString()
  const args: ScanCommandInput = {
    TableName: TestTable,
    FilterExpression: 'Creation < :expired',
    ExpressionAttributeValues: { ':expired': { N: expired } },
  }
  return dynamoScan(args, pageSize)
}

export const deleteStreamReq = async (streamId: StreamID) => {
  try {
    return await DynamoClient.deleteItem({
      TableName: TestTable,
      Key: {
        StreamId: {
          S: streamId.toString(),
        },
      },
    })
  } catch (err) {
    throw 'unexpected error: ' + err
  }
}

export const markStreamReqAsAnchored = async (streamId: StreamID) => {
  try {
    return await DynamoClient.updateItem({
      TableName: TestTable,
      Key: {
        StreamId: {
          S: streamId.toString(),
        },
      },
      UpdateExpression: 'set Anchored = :anchored',
      ExpressionAttributeValues: { ':anchored': { BOOL: true } },
    })
  } catch (err) {
    throw 'unexpected error: ' + err
  }
}

/**
 * Should not be used in production code, this is just for testing purposes.
 * Modifies the "Creation" field in a database record so that it appears as if that
 * stream was created longer ago than StreamTTL, and thus will show up as expired and in need of
 * garbage collection.
 */
export const makeStreamReqAppearExpired_FOR_TESTING_ONLY = async (streamId: StreamID) => {
  try {
    const now = DateTime.utc()
    const expired = now.minus(StreamTTL).minus(Duration.fromMillis(1000)).toSeconds().toString()

    return await DynamoClient.updateItem({
      TableName: TestTable,
      Key: {
        StreamId: {
          S: streamId.toString(),
        },
      },
      UpdateExpression: 'set Creation = :creation',
      ExpressionAttributeValues: { ':creation': { N: expired } },
    })
  } catch (err) {
    throw 'unexpected error: ' + err
  }
}

// we want to avoid trying to load too many database records into memory all at once
const MAX_SCAN_RESULTS = 10 * 1000
/**
 * Issues a scan query with the given arguments against dynamoDB.
 * pageSize controls how many records it will scan in a single batch (applied *before* matching
 * any provided filters).  It is not a limit on the total size of the result set as this function
 * will handle processing multiple batches until all results have been found or we hit the limit of
 * MAX_SCAN_RESULTS.
 * Note that if we start seeing failures due to exhausting our RCUs (Read Capacity Units per second),
 * and getting throttled by Amazon, decreasing the pageSize we use may help keep us under the limit.
 */
const dynamoScan = async (args: ScanCommandInput, pageSize: number | null = null) => {
  if (pageSize) {
    args.Limit = pageSize
  }
  try {
    const results = []
    let lastEvaluatedKey = null
    while (results.length < MAX_SCAN_RESULTS) {
      if (lastEvaluatedKey) {
        args.ExclusiveStartKey = lastEvaluatedKey // start from where previous scan left off
      }

      const scanResult = await DynamoClient.scan(args)
      if (!scanResult.Items) {
        return results
      }
      results.push(...scanResult.Items)

      if (scanResult.LastEvaluatedKey) {
        // there are more results
        lastEvaluatedKey = scanResult.LastEvaluatedKey
      } else {
        return results
      }
    }
    return results
  } catch (err) {
    throw 'unexpected error: ' + err
  }
}
