# Configuration

Configuration is done via environment variables and flags.

See the [Daemon](./help-daemon.md) section for more information on how to use the `ceramic-one` flags.

The following environment variables are important, refer to the page above for the complete list.

- `CERAMIC_ONE_NETWORK`: Ceramic network (default: `testnet-clay`)
- `CERAMIC_ONE_STORE_DIR`: Path to storage directory (default: `~/.ceramic-one`)
- `CERAMIC_ONE_BIND_ADDRESS`: Bind address of the metrics endpoint (default: `5001`)
- `CERAMIC_ONE_METRICS_BIND_ADDRESS`: Bind address of the metrics endpoint (default: `9464`)
- `CERAMIC_ONE_SWARM_ADDRESSES`: Listen address of the p2p swarm (default: `/ip4/0.0.0.0/tcp/4001`)
- `CERAMIC_ONE_LOCAL_NETWORK_ID`

