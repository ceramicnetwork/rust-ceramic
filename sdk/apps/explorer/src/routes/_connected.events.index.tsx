import { Alert } from '@mantine/core'

import { createFileRoute } from '@tanstack/react-router'

export const Route = createFileRoute('/_connected/events/')({
  component: () => (
    <Alert color="blue">Select an event to display details</Alert>
  ),
})
