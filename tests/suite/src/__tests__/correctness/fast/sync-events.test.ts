import { beforeAll, describe, expect, test } from '@jest/globals'
import { utilities } from '../../../utils/common.js'
import fetch from 'cross-fetch'
import { randomCID } from '@ceramicnetwork/streamid'
import { StreamID } from '@ceramic-sdk/identifiers'
import { ReconEvent, ReconEventInput, randomEvents, registerInterest } from '../../../utils/rustCeramicHelpers.js'

const delayMs = utilities.delayMs
// Environment variables
const CeramicUrls = String(process.env.CERAMIC_URLS).split(',')
const READ_EVENTS_TIMEOUT_MS = 60 * 1000


async function getStartingToken(url: string): Promise<string> {
  const tokenResponse = await fetch(url + `/ceramic/feed/resumeToken`, { method: 'GET' })
  if (tokenResponse.status !== 200) {
    const data = await tokenResponse.text()
    console.warn(`resumeToken: node: ${url}, result: ${data}`)
  }
  expect(tokenResponse.status).toEqual(200)
  const token = await tokenResponse.json()
  return token.resumeToken
}

async function getResumeTokens(urls: string[]): Promise<string[]> {
  const resumeTokens = []
  for (const url of urls) {
    const token = await getStartingToken(url)
    resumeTokens.push(token)
  }
  return resumeTokens
}

async function writeEvents(url: string, events: ReconEventInput[]) {
  for (const event of events) {
    let response = await fetch(url + '/ceramic/events', {
      method: 'POST',
      body: JSON.stringify(event),
    })
    if (response.status !== 202) {
      const data = await response.text()
      console.warn(`writeEvents: node ${url}, result: ${data}`)
    }
    expect(response.status).toEqual(202)
  }
}

async function getEventData(url: string, eventId: string): Promise<ReconEvent> {
  const fullUrl = url + `/ceramic/events/${eventId}`
  const response = await fetch(fullUrl)
  expect(response.status).toEqual(200)
  return response.json()
}

async function readEvents(
  url: string,
  resumeToken: string,
  eventCids: string[],
  timeoutMs = READ_EVENTS_TIMEOUT_MS,
) {
  const events = []
  console.log(
    `readEvents: ${url} starting at ${resumeToken}, waiting for ${eventCids} events`,
  )
  var startTime = Date.now()
  while (events.length < eventCids.length) {
    if (Date.now() - startTime > timeoutMs) {
      // if it took more than a minute, quit
      console.warn(
        `readEvents: timeout after ${timeoutMs} millis waiting for ${eventCids} but only ${events.length} events found`,
      )
      break
    }

    const fullUrl = url + `/ceramic/feed/events?resumeAt=${resumeToken}`
    const response = await fetch(fullUrl)
    expect(response.status).toEqual(200)
    const data = await response.json()
    resumeToken = data.resumeToken

    for (const event of data.events) {
      const eventWithData = await getEventData(url, event.id)
      if (eventCids.includes(eventWithData.id)) {
        events.push(eventWithData)
      }
    }

    await delayMs(100)
  }
  return sortModelEvents(events) // sort so that tests are stable
}

function sortModelEvents(events: ReconEvent[]): ReconEvent[] {
  if (events && events.length > 0) {
    return JSON.parse(JSON.stringify(events)).sort((a: any, b: any) => {
      if (a.id > b.id) return 1
      if (a.id < b.id) return -1
      return 0
    })
  } else {
    return []
  }
}

// Wait up till retries seconds for all urls to have at least count events
async function waitForEventCount(urls: string[], eventCids: string[], resumeTokens: string[]) {
  if (urls.length !== resumeTokens.length) {
    throw new Error('The lengths of urls and resumeTokens arrays must be equal')
  }
  let all_good = true
  for (let i = 0; i < urls.length; i++) {
    let url = urls[i]
    let events = await readEvents(url, resumeTokens[i], eventCids)
    if (events.length < eventCids.length) {
      all_good = false
      break
    }
  }
  if (all_good) {
    return
  }
  throw new Error(`waitForEventCount: timeout`)
}

describe('sync events', () => {
  const firstNodeUrl = CeramicUrls[0]
  const secondNodeUrl = CeramicUrls[1]

  beforeAll(() => {
    if (!firstNodeUrl || !secondNodeUrl) {
      throw new Error('CERAMIC_URLS environment variable must be set')
    }
  })

  test(`linear sync on ${firstNodeUrl}`, async () => {
    const modelID = new StreamID('model', randomCID())
    let modelEvents = randomEvents(modelID, 10)
    const resumeTokens: string[] = await getResumeTokens(CeramicUrls)

    // Write all data to one node before subscribing on the other nodes.
    // This way the other nodes to a linear download of the data from the first node.
    await registerInterest(firstNodeUrl, modelID)
    await writeEvents(firstNodeUrl, modelEvents)

    // Now subscribe on the other nodes
    for (let idx = 1; idx < CeramicUrls.length; idx++) {
      let url = CeramicUrls[idx]
      await registerInterest(url, modelID)
    }
    const sortedModelEvents = sortModelEvents(modelEvents)
    const eventCids = modelEvents.map((e) => e.id)
    await waitForEventCount(CeramicUrls, eventCids, resumeTokens)

    // Use a sorted expected value for stable tests
    // Validate each node got the events, including the first node
    for (let i = 0; i < CeramicUrls.length; i++) {
      const url = CeramicUrls[i]
      const events = await readEvents(url, resumeTokens[i], eventCids)

      expect(events).toEqual(sortedModelEvents)
    }
  })

  test(`active write sync on ${firstNodeUrl}`, async () => {
    const modelID = new StreamID('model', randomCID())
    let modelEvents = randomEvents(modelID, 10)
    const resumeTokens: string[] = await getResumeTokens(CeramicUrls)

    // Subscribe on all nodes then write the data
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      await registerInterest(url, modelID)
    }
    await writeEvents(firstNodeUrl, modelEvents)
    const eventCids = modelEvents.map((e) => e.id)

    await waitForEventCount(CeramicUrls, eventCids, resumeTokens)

    // Use a sorted expected value for stable tests
    const sortedModelEvents = sortModelEvents(modelEvents)
    // Validate each node got the events, including the first node
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      let events = await readEvents(url, resumeTokens[idx], eventCids)

      expect(events).toEqual(sortedModelEvents)
    }
  })
  test(`half and half sync on ${firstNodeUrl}`, async () => {
    const modelID = new StreamID('model', randomCID())
    let modelEvents = randomEvents(modelID, 20)
    const resumeTokens: string[] = await getResumeTokens(CeramicUrls)

    // Write half the data before other nodes subscribe
    await registerInterest(firstNodeUrl, modelID)
    let half = Math.ceil(modelEvents.length / 2)
    let firstHalf = modelEvents.slice(0, half)
    let secondHalf = modelEvents.slice(half, modelEvents.length)
    await writeEvents(firstNodeUrl, firstHalf)

    // Now subscribe on the other nodes
    for (let idx = 1; idx < CeramicUrls.length; idx++) {
      let url = CeramicUrls[idx]
      await registerInterest(url, modelID)
    }
    // Write the second half of the data
    await writeEvents(firstNodeUrl, secondHalf)
    const eventCids = modelEvents.map((e) => e.id)

    await waitForEventCount(CeramicUrls, eventCids, resumeTokens)

    // Use a sorted expected value for stable tests
    const sortedModelEvents = sortModelEvents(modelEvents)
    // Validate each node got the events, including the first node
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      let events = await readEvents(url, resumeTokens[idx], eventCids)

      expect(events).toEqual(sortedModelEvents)
    }
  })
  test(`active write sync on two nodes ${firstNodeUrl} ${secondNodeUrl}`, async () => {
    const modelID = new StreamID('model', randomCID())
    let modelEvents = randomEvents(modelID, 20)
    let half = Math.ceil(modelEvents.length / 2)
    let firstHalf = modelEvents.slice(0, half)
    let secondHalf = modelEvents.slice(half, modelEvents.length)

    const resumeTokens: string[] = await getResumeTokens(CeramicUrls)

    // Subscribe on all nodes then write the data
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      await registerInterest(url, modelID)
    }

    // Write to both node simultaneously
    await Promise.all([
      writeEvents(firstNodeUrl, firstHalf),
      writeEvents(secondNodeUrl, secondHalf),
    ])
    const eventCids = modelEvents.map((e) => e.id)

    await waitForEventCount(CeramicUrls, eventCids, resumeTokens)

    // Use a sorted expected value for stable tests
    const sortedModelEvents = sortModelEvents(modelEvents)
    // Validate each node got the events, including the first node
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      let events = await readEvents(url, resumeTokens[idx], eventCids)

      expect(events).toEqual(sortedModelEvents)
    }
  })
  test(`active write for many models sync`, async () => {
    const modelCount = 10
    const models = []
    const allEvents: ReconEvent[] = []
    for (let m = 0; m < modelCount; m++) {
      let modelID = new StreamID('model', randomCID())
      // Generate random events for each model for each node
      let events = []
      for (let _ in CeramicUrls) {
        const newEvents = randomEvents(modelID, 2)
        events.push(newEvents)
        allEvents.push(...newEvents)
      }
      models.push({
        id: modelID,
        events: events,
      })
    }

    const resumeTokens: string[] = await getResumeTokens(CeramicUrls)
    // Subscribe on all nodes to all models then write the data
    for (let m in models) {
      let model = models[m]
      for (let idx in CeramicUrls) {
        let url = CeramicUrls[idx]
        await registerInterest(url, model.id)
      }
    }

    // Write to all nodes for all models simultaneously
    let writes = []
    for (let m in models) {
      let model = models[m]
      for (let idx in CeramicUrls) {
        let url = CeramicUrls[idx]
        writes.push(writeEvents(url, model.events[idx]))
      }
    }
    await Promise.all(writes)
    const eventCids = allEvents.map((e) => e.id)

    await waitForEventCount(CeramicUrls, eventCids, resumeTokens)

    // Use a sorted expected value for stable tests
    const sortedModelEvents = sortModelEvents(allEvents)
    // Validate each node got the events, including the first node
    for (let idx in CeramicUrls) {
      let url = CeramicUrls[idx]
      const foundEvents = await readEvents(url, resumeTokens[idx], eventCids)

      expect(foundEvents).toEqual(sortedModelEvents)
    }
  })
})
