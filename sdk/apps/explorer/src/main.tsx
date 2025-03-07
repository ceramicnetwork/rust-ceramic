import '@mantine/core/styles.css'
import { MantineProvider, createTheme } from '@mantine/core'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  RouterProvider,
  createHashHistory,
  createRouter,
} from '@tanstack/react-router'
import { Provider as JotaiProvider } from 'jotai'
import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { routeTree } from './routeTree.gen.ts'
import { store } from './state.ts'

const queryClient = new QueryClient()

const router = createRouter({
  context: { queryClient },
  history: createHashHistory(),
  defaultPreloadStaleTime: 0,
  routeTree,
})

const theme = createTheme({
  primaryColor: 'orange',
})

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

// biome-ignore lint/style/noNonNullAssertion: element exists
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <JotaiProvider store={store}>
      <QueryClientProvider client={queryClient}>
        <MantineProvider theme={theme}>
          <RouterProvider router={router} />
        </MantineProvider>
      </QueryClientProvider>
    </JotaiProvider>
  </StrictMode>,
)
