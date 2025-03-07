import { Box, Button, Code } from '@mantine/core'
import { useClipboard } from '@mantine/hooks'
import { IconCheck, IconCopy } from '@tabler/icons-react'

import classes from './CopyCodeBlock.module.css'

export type Props = {
  code: string
}

export default function CopyCodeBlock(props: Props) {
  const clipboard = useClipboard()

  return (
    <Box className={classes.container}>
      <Button
        color="gray"
        variant="transparent"
        className={classes.button}
        onClick={() => clipboard.copy(props.code)}>
        {clipboard.copied ? <IconCheck /> : <IconCopy />}
      </Button>
      <Code block>{props.code}</Code>
    </Box>
  )
}
