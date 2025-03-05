import type { CeramicClient } from '@ceramic-sdk/http-client'
import { useInfiniteQuery, useQuery } from '@tanstack/react-query'
import { useAtomValue } from 'jotai'

import { decodeEvent } from './events.ts'
import { ceramicClientAtom } from './state.ts'

export function useClientQuery<T>(
  key: Array<string>,
  executeQuery: (client: CeramicClient) => Promise<T>,
) {
  const client = useAtomValue(ceramicClientAtom)
  return useQuery({
    queryKey: key,
    queryFn: (): Promise<T> => executeQuery(client),
  })
}

export function useEventContainer(id: string) {
  return useClientQuery(['events', id], async (client) => {
    const event = await client.getEventData(id)
    return await decodeEvent(event)
  })
}

export function useEventsFeed() {
  const client = useAtomValue(ceramicClientAtom)
  return useInfiniteQuery({
    queryKey: ['events'],
    queryFn: ({ pageParam }: { pageParam: string | undefined }) => {
      return client.getEventsFeed({ resumeAt: pageParam })
    },
    initialPageParam: undefined,
    getNextPageParam: (lastPage) => lastPage.resumeToken,
    maxPages: 10,
    refetchInterval: 10_000, // 10 seconds
  })
}
