import { Alert, Loader } from '@mantine/core'
import { useQueryErrorResetBoundary } from '@tanstack/react-query'
import { createFileRoute, useRouter } from '@tanstack/react-router'
import { useEffect } from 'react'

import ConnectInstructions from '../components/ConnectInstructions.tsx'
import { getCeramicClient } from '../state.ts'

export const Route = createFileRoute('/_connected')({
  loader: async ({ context }) => {
    return await context.queryClient.ensureQueryData({
      queryKey: ['version'],
      queryFn: async () => {
        const result = await getCeramicClient().getVersion()
        return result.version
      },
      retry: false,
    })
  },
  errorComponent: () => {
    // See https://tanstack.com/router/latest/docs/framework/react/guide/external-data-loading#error-handling-with-tanstack-query
    const router = useRouter()
    const queryErrorResetBoundary = useQueryErrorResetBoundary()

    useEffect(() => {
      queryErrorResetBoundary.reset()
    }, [queryErrorResetBoundary])

    return <ConnectInstructions onClickRetry={() => router.invalidate()} />
  },
  pendingComponent: () => (
    <Alert
      color="blue"
      title="Loading Ceramic node version..."
      icon={<Loader size="sm" color="blue" />}
    />
  ),
})
