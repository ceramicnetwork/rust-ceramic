# Rust Ceramic


Implementation of the Ceramic protocol in Rust.

## Usage

For now this repo only replaces the p2p bits from iroh.
To run all iroh services follow these steps:

Start this modified p2p process

    $ cd rust-ceramic
    $ cargo run

Next run the unmodified store and gateway services

    $ git clone https://github.com/n0-computer/iroh.git
    $ cd iroh
    $ cargo run --bin iroh-store &
    $ cargo run --bin iroh-gateway &

Finally check that all services are running:

    $ cargo run --bin iroh -- status

You should see the `p2p` service log some Ceramic messages.

## License

Fully open source and dual-licensed under MIT and Apache 2.
