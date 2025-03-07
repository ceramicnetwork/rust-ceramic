FROM node:20

RUN npm install -g pnpm@9

RUN apt-get update && apt-get install -y \
    curl \
    jq \
    bash \
    build-essential

# Get Rust
ENV PATH="/root/.cargo/bin:${PATH}"
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y && rustup install stable

# Build and install the sdk
WORKDIR /app/sdk
COPY sdk .
RUN pnpm install && pnpm build

# Install the test suite
WORKDIR /app/tests/suite

COPY tests/suite/package.json tests/suite/pnpm-*.yaml tests/suite/jest.config.json tests/suite/tsconfig.json tests/suite/entrypoint.sh .

# Install all npm dependencies including dev dependencies
RUN pnpm install

COPY tests/suite/src ./src

# Select all tests by default
ENV TEST_SELECTOR="correctness"

ENTRYPOINT /app/tests/suite/entrypoint.sh run test --testPathPattern ${TEST_SELECTOR}
