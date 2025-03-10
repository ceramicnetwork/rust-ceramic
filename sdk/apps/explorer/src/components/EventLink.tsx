import { Anchor, Tooltip } from '@mantine/core'
import { Link } from '@tanstack/react-router'

export type Props = {
  id: string
}

export default function EventLink({ id }: Props) {
  return (
    <Tooltip label={id}>
      <Anchor
        component={Link}
        style={{ fontFamily: 'monospace' }}
        to={`/events/${id}`}>
        {id}
      </Anchor>
    </Tooltip>
  )
}
