import { AppShell, Container, Group, Image, Title } from '@mantine/core'
import type { QueryClient } from '@tanstack/react-query'
import { Outlet, createRootRouteWithContext } from '@tanstack/react-router'

import ceramicLogo from '../../assets/ceramic.svg'

export const Route = createRootRouteWithContext<{
  queryClient: QueryClient
}>()({
  component: () => (
    <AppShell header={{ height: 70 }} padding="md">
      <AppShell.Header>
        <Container p="sm" size="lg">
          <Group>
            <Image src={ceramicLogo} h="45px" w="45px" />
            <Title>Ceramic Explorer</Title>
          </Group>
        </Container>
      </AppShell.Header>
      <AppShell.Main>
        <Container size="lg">
          <Outlet />
        </Container>
      </AppShell.Main>
    </AppShell>
  ),
})
