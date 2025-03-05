import { createFileRoute } from '@tanstack/react-router'

import EventDetails from '../components/EventDetails.tsx'
import { decodeEvent } from '../events.ts'
import { getCeramicClient } from '../state.ts'

function getQueryOptions(id: string) {
  return {
    queryKey: ['events', id],
    queryFn: async () => {
      const event = await getCeramicClient().getEventData(id)
      return await decodeEvent(event)
    },
  }
}

export const Route = createFileRoute('/_connected/events/$id')({
  loader: async ({ context, params: { id } }) => {
    return await context.queryClient.ensureQueryData(getQueryOptions(id))
  },
  component: () => {
    const { id } = Route.useParams()
    const event = Route.useLoaderData()
    return <EventDetails event={event.payload} id={id} />
  },
})
