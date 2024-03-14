# Usage

## Run a daemon process

Start a daemon process for use by a js-ceramic instance.

See [Configuration](./configuration.md) for more information on how to customize the daemon.

The default configuration:
- uses the testnet-clay network
- binds to `4001` for peer-to-peer communication
- binds to `5001` for the HTTP API (private)
- binds to `9464` for metrics (private)
- writes data to `~/.ceramic-one/`

See the [configuration](./configuration.md) section for more information on how to customize the daemon.

Run it with:
```
ceramic-one daemon
```

## Event store tools

```
ceramic-one events
```