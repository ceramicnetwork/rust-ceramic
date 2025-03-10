import { TimeEvent } from '@ceramic-sdk/events'
import {
  DocumentDataEventPayload,
  DocumentInitEventPayload,
  getStreamID,
} from '@ceramic-sdk/model-instance-protocol'
import {
  ModelInitEventPayload,
  getModelStreamID,
} from '@ceramic-sdk/model-protocol'
import {
  Alert,
  Badge,
  Code,
  ScrollArea,
  Stack,
  Text,
  Tooltip,
} from '@mantine/core'
import {
  IconClock,
  IconCodeDots,
  IconExclamationCircle,
  IconFilePencil,
  IconFilePlus,
} from '@tabler/icons-react'
import { CID } from 'multiformats'
import { memo } from 'react'

import type { SupportedPayload } from '../events.ts'
import { useEventContainer } from '../hooks.ts'

import CopyCodeBlock from './CopyCodeBlock.tsx'
import EventLink from './EventLink.tsx'

function InlineID({ value }: { value: string }) {
  return (
    <Tooltip label={value}>
      <Text span style={{ fontFamily: 'monospace' }}>
        {value}
      </Text>
    </Tooltip>
  )
}

function PayloadBlock({ value }: { value: unknown }) {
  return (
    <ScrollArea.Autosize mah="400px">
      <Code block>{JSON.stringify(value, null, 2)}</Code>
    </ScrollArea.Autosize>
  )
}

function TimeEventStreamID({ id }: { id: string }) {
  const result = useEventContainer(id)
  if (result.data == null) {
    return <Text span>loading...</Text>
  }
  if (ModelInitEventPayload.is(result.data.payload)) {
    return <InlineID value={getModelStreamID(CID.parse(id)).toString()} />
  }
  if (DocumentInitEventPayload.is(result.data.payload)) {
    return <InlineID value={getStreamID(CID.parse(id)).toString()} />
  }
  return <Text span>unsupported</Text>
}

export type Props = {
  id: string
  event: SupportedPayload
}

function EventDetails(props: Props) {
  if (TimeEvent.is(props.event)) {
    const event = props.event as TimeEvent

    return (
      <Stack>
        <Badge size="lg" leftSection={<IconClock />}>
          Time event
        </Badge>
        <Text truncate>
          StreamID: <TimeEventStreamID id={event.id.toString()} />
        </Text>
        <Text truncate>
          Init event: <EventLink id={event.id.toString()} />
        </Text>
        <Text truncate>
          Previous event: <EventLink id={event.prev.toString()} />
        </Text>
        <Text>Path: {event.path}</Text>
        <Text>
          Proof: <InlineID value={event.proof.toString()} />
        </Text>
      </Stack>
    )
  }

  if (ModelInitEventPayload.is(props.event)) {
    const event = props.event as ModelInitEventPayload
    const streamID = getModelStreamID(CID.parse(props.id)).toString()

    return (
      <Stack>
        <Badge size="lg" leftSection={<IconCodeDots />}>
          Model init event
        </Badge>
        <Text truncate>
          StreamID: <InlineID value={streamID} />
        </Text>
        <Text>Register interest:</Text>
        <CopyCodeBlock
          code={`curl -X POST "http://localhost:5101/ceramic/interests/model/${streamID}"`}
        />
        <Text>Payload data:</Text>
        <PayloadBlock value={event.data} />
      </Stack>
    )
  }

  if (DocumentInitEventPayload.is(props.event)) {
    const event = props.event as DocumentInitEventPayload
    const modelCID = event.header.model.baseID.cid.toString()
    const streamID = getStreamID(CID.parse(props.id)).toString()

    return (
      <Stack>
        <Badge size="lg" leftSection={<IconFilePlus />}>
          Document init event
        </Badge>
        <Text truncate>
          StreamID: <InlineID value={streamID} />
        </Text>
        <Text truncate>
          Model: <InlineID value={event.header.model.toString()} />
        </Text>
        <Text truncate>
          Model init event: <EventLink id={modelCID} />
        </Text>
        <Text truncate>
          Controller: <InlineID value={event.header.controllers[0]} />
        </Text>
        <Text>Payload data:</Text>
        <PayloadBlock value={event.data} />
      </Stack>
    )
  }

  if (DocumentDataEventPayload.is(props.event)) {
    const event = props.event as DocumentDataEventPayload
    const streamID = getStreamID(event.id).toString()

    return (
      <Stack>
        <Badge size="lg" leftSection={<IconFilePencil />}>
          Document data event
        </Badge>
        <Text truncate>
          StreamID: <InlineID value={streamID} />
        </Text>
        <Text truncate>
          Init event: <EventLink id={event.id.toString()} />
        </Text>
        <Text truncate>
          Previous event: <EventLink id={event.prev.toString()} />
        </Text>
        <Text>Payload data:</Text>
        <PayloadBlock value={event.data} />
      </Stack>
    )
  }

  return (
    <Alert
      color="red"
      title="Unknown event type"
      icon={<IconExclamationCircle />}
    />
  )
}

export default memo(EventDetails)
