# Upgrade to Ceramic One from Kubo

This document describes how to upgrade an existing Ceramic node using Kubo to run with `ceramic-one` while preserving existing data.

## Overview

Follow these steps for basic upgrade of a Ceramic node with some downtime.

1. Shutdown existing `js-ceramic` node.
2. Migrate data from IPFS blocks into Sqlite database file `ceramic-one` can consume.
3. Start `ceramic-one` daemon.
4. Configure `js-ceramic` to run against `ceramic-one`.
5. Start `js-ceramic` node and validate.

If you require a zero downtime upgrade see the section below that expands the basic steps for a near zero downtime upgrade.

### Shutdown Existing Node

First shutdown both the `js-ceramic` and `kubo` processes using whatever mechanism your infrastructure dictates.

### Migrate IPFS Blocks to Ceramic Events

Kubo stores data as IPFS blocks on disk.
If you are not using the disk backed Kubo storage first migrate data to an on disk store following Kubo's instructions.

These IPFS blocks are the raw pieces of Ceramic Events.
In order to preserve this data we need to migrate them into Ceramic Events stored in as a Sqlite database file.

The `ceramic-one` command contains a subcommand for migrating data from IPFS blocks.
Install the `ceramic-one` binary following the instructions in README.md.

See the complete usage information for the migration command:

    ceramic-one migrations from-ipfs --help

The command requires an input directory where the IPFS blocks are found and where the output storage directory.

The following command will migrate data assuming the default directories for both Kubo and `ceramic-one`.
Adjust the command arguments as needed by your infrastructure.

    ceramic-one migrations from-ipfs -i ~/.ipfs/blocks -o ~/.ceramic-one --network mainnet

This command will migrate all blocks stored in the `~/.ipfs/blocks` directory into the database file `~/.ceramic-one/db.sqlite3` for the `mainnet` network.
As output from the command you should see a total number of events migrated, this will not be the same as the number of blocks as multiple blocks construct into a single event.
You will also see a number of errors. This represents the number of blocks found that are part of a Ceramic Event but for whatever reasons could not be constructed into a complete event.
Any error will be logged for investigations.
Any blocks stored in Kubo that are not Ceramic Events will be ignored.

### Start `ceramic-one` Daemon

Once the data is migrated its time to start the node again.
The `ceramic-one` process is started in place of the `kubo` process, Kubo is no longer needed.

Read the full usage of the `ceramic-one` daemon:

    ceramic-one daemon --help

Once you are familiar with configuring the daemon start it with defaults or specify any arguments specific to your infrastructure.

    ceramic-one daemon

You should see logs indicating the daemon is running.

### Configure `js-ceramic` for `ceramic-one`

First change the `ipfs` section in the `daemon.config.json` to use `remote` mode and point it at the running `ceramic-one` daemon.
By default the `ceramic-one` daemon listens on port 5001, if you configured it differently make sure to match that configuration for `js-ceramic` as well.

```josn
"ipfs": {
    "mode": "remote",
    "host": "http://localhost:5001"
  },
```

Second, set and environment variable for the `js-ceramic` process so it knows its talking to `ceramic-one` and not Kubo.

    CERAMIC_RECON_MODE=true

Both of these configuration options may not be required in the future, however for now they are.


### Start `js-ceramic` Node and Validate

Now start the `js-ceramic` node with its new configuration and environment.
Validate the process starts as before and that your application can communicate with it as needed.

## Recovery

At this point you should have a working upgraded `js-ceramic` node but sometimes things go wrong.
The Ceramic node does some basic checks on startup to ensure its various data store are in sync, if these check fail the process with exit with an error message about missing data.
In that case or any other failure mode follow these steps to return your node to its working state.

1. Stop the `ceramic-one` process.
2. Revert the configuration and environment changes to `js-ceramic`.
3. Start `js-ceramic` process.

## Live Upgrade Steps

If you cannot tolerate a window of downtime of o your Ceramic node you can perform a two pass migration process to minimize the amount of time your node is offline.
The basic strategy here is to run two nodes behind a proxy in an active/passive mode.

These steps are very similar to the normal upgrade process so please review those steps first.
The high level sequence is:

1. Migrate the data from the active old node.
2. Start new node as the passive with the migrated data.
3. Validate new passive node is working
4. Switch active role to new node and passive role to old node.
5. Migrate data from the old node to the new node again to catch any new data since the first migration.
6. Shutdown old node as it is now behind and out of date.

The `ceramic-one migrations from-ipfs` command can be run against a running `ceramic-one` daemon process.
It will cause contention on the database, as a result traffic to the node may be impacted, but no downtime is required.
