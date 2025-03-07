import {
  Alert,
  Grid,
  NavLink,
  Stack,
  Text,
  Title,
  Tooltip,
} from '@mantine/core'
import { IconChevronRight } from '@tabler/icons-react'
import { Link, Outlet } from '@tanstack/react-router'

import CopyCodeBlock from '../components/CopyCodeBlock.tsx'
import { useEventsFeed } from '../hooks.ts'

const CURL_COMMAND =
  'curl -X POST "http://localhost:5101/ceramic/interests/model/kh4q0ozorrgaq2mezktnrmdwleo1d"'

import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/_connected/events')({
  component: EventsFeed,
})

function EventsFeed() {
  const feed = useEventsFeed()
  const pages = feed.data?.pages ?? []
  const firstPageEvents = pages[0]?.events

  if (firstPageEvents == null) {
    return null
  }

  if (firstPageEvents.length === 0) {
    return (
      <Alert color="orange" title="No events">
        <Text>No events are stored on the Ceramic node yet!</Text>
        <Text>
          To start receiving events for the models created on the network, run
          the following command:
        </Text>
        <CopyCodeBlock code={CURL_COMMAND} />
      </Alert>
    )
  }

  const items = [...pages]
    .reverse()
    .flatMap((page) => [...page.events].reverse())
    .map((event) => {
      return (
        <Tooltip key={event.id} label={event.id}>
          <NavLink
            component={Link}
            to="/events/$id"
            params={{ id: event.id }}
            label={event.id}
            style={{ fontFamily: 'monospace' }}
            rightSection={<IconChevronRight />}
          />
        </Tooltip>
      )
    })

  return (
    <>
      <Title order={2}>Events</Title>
      <Grid mt="sm">
        <Grid.Col
          span={4}
          style={{ height: 'calc(100vh - 160px)', overflow: 'auto' }}>
          <Stack gap="xs">{items}</Stack>
        </Grid.Col>
        <Grid.Col span={8}>
          <Outlet />
        </Grid.Col>
      </Grid>
    </>
  )
}
