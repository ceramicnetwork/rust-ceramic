FROM node:20-alpine

RUN npm install -g pnpm@8

WORKDIR /app

RUN apk add --no-cache curl jq bash

COPY package.json pnpm-*.yaml jest.config.json tsconfig.json entrypoint.sh ./

# Install all npm dependencies including dev dependencies
RUN pnpm install

# Copy all files not excluded by .dockerignore of current directory to docker container
COPY src/ ./src

# Select all tests by default
ENV TEST_SELECTOR="correctness"

ENTRYPOINT ["/app/entrypoint.sh", "run", "test", "--testPathPattern", "$TEST_SELECTOR"]
