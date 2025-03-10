import { Alert, Anchor, Button, Code, Tabs, Text } from '@mantine/core'
import { useOs } from '@mantine/hooks'
import { IconExclamationCircle } from '@tabler/icons-react'
import { useState } from 'react'

import CopyCodeBlock from './CopyCodeBlock.tsx'

import classes from './ConnectInstructions.module.css'

const DEBIAN_INSTALLATION = `# get deb.tar.gz
curl -LO https://github.com/ceramicnetwork/rust-ceramic/releases/download/latest/ceramic-one_x86_64-unknown-linux-gnu.tar.gz
# untar the Debian software package file
tar zxvf ceramic-one_x86_64-unknown-linux-gnu.tar.gz
# install with dpkg - package manager for Debian
dpkg -i ceramic-one.deb`

export type Props = {
  onClickRetry: () => void
}

export default function ServerConnectedContainer({ onClickRetry }: Props) {
  const os = useOs()
  const [activeTab, setActiveTab] = useState<string | null>(null)

  return (
    <>
      <Alert
        color="red"
        title="Failed to access Ceramic node"
        icon={<IconExclamationCircle />}>
        <Text>
          Make sure Ceramic One is running on <Code>http://localhost:5101</Code>
          . If Ceramic One is not installed yet, please follow the instructions
          below.
        </Text>
        <Button onClick={onClickRetry}>Retry</Button>
      </Alert>
      <Tabs
        value={activeTab ?? os}
        onChange={setActiveTab}
        classNames={{ root: classes.tabsRoot, panel: classes.marginTop }}>
        <Tabs.List>
          <Tabs.Tab value="macos">macOS</Tabs.Tab>
          <Tabs.Tab value="linux">Linux</Tabs.Tab>
          <Tabs.Tab value="windows">Windows</Tabs.Tab>
          <Tabs.Tab value="docker">Docker</Tabs.Tab>
        </Tabs.List>

        <Tabs.Panel value="macos">
          <Text>
            Install using{' '}
            <Anchor href="https://brew.sh/" target="_blank">
              Homebrew
            </Anchor>
            :
          </Text>
          <CopyCodeBlock code="brew install ceramicnetwork/tap/ceramic-one" />
          <Text className={classes.marginTop}>
            Once Ceramic One is installed, run the daemon:
          </Text>
          <CopyCodeBlock code="ceramic-one daemon" />
        </Tabs.Panel>

        <Tabs.Panel value="linux">
          <Text>
            For Debian-based distributions, install a the latest release using
            dpkg:
          </Text>
          <CopyCodeBlock code={DEBIAN_INSTALLATION} />
          <Text className={classes.marginTop}>
            To build from source, follow the{' '}
            <Anchor
              href="https://github.com/ceramicnetwork/rust-ceramic?tab=readme-ov-file#linux---from-source"
              target="_blank">
              instructions from the Ceramic One repository
            </Anchor>
            .
          </Text>
          <Text className={classes.marginTop}>
            Once Ceramic One is installed, run the daemon:
          </Text>
          <CopyCodeBlock code="ceramic-one daemon" />
        </Tabs.Panel>

        <Tabs.Panel value="windows">
          <Text>Ceramic One does not directly support Windows yet.</Text>
          <Text className={classes.marginTop}>
            See the{' '}
            <Text
              span
              c="orange"
              style={{ cursor: 'pointer' }}
              onClick={() => setActiveTab('docker')}>
              Docker instructions
            </Text>{' '}
            to run a Ceramic One container.
          </Text>
        </Tabs.Panel>

        <Tabs.Panel value="docker">
          <Text>
            Run Ceramic One using{' '}
            <Anchor href="https://www.docker.com/" target="_blank">
              Docker
            </Anchor>
            :
          </Text>
          <CopyCodeBlock code="docker run --network=host public.ecr.aws/r5b3e0r5/3box/ceramic-one:latest" />
          <Text className={classes.marginTop}>
            To use Docker Compose, follow the{' '}
            <Anchor
              href="https://github.com/ceramicnetwork/rust-ceramic?tab=readme-ov-file#docker-compose"
              target="_blank">
              instructions from the Ceramic One repository
            </Anchor>
            .
          </Text>
        </Tabs.Panel>
      </Tabs>
    </>
  )
}
