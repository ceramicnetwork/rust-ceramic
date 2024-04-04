#!/usr/bin/env bash

# Script to generate the offline sqlx query data
# creates and delete sqlite file
# create postgres db and tear it down
# support postgres and sqlite

function prepare_database() {
    echo "Applying migrations at $2 to database $1"
    if [ -z "$CI_RUN" ]; then
        cargo sqlx database reset --database-url "$1"
    else 
        cargo sqlx database reset --database-url "$1" -y
    fi 
    cargo sqlx database setup --database-url "$1" --source "$2"
    cd "$CI_DIR/../store"
    if [ -z "$CI_RUN" ]; then
        cargo sqlx prepare --database-url "$1"
    else
        cargo sqlx prepare --database-url "$1" --check
    fi
}

set -e

DATABASE=$1

if [ -z "$DATABASE" ]; then
  echo "Parameter required. Should be 'sqlite' or 'postgres'"
  exit 1
fi

CI_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$CI_DIR/.."

cargo install sqlx-cli

# case statement to check if the database is sqlite or postgres
case $DATABASE in
  postgres*)
    echo "Using postgres"
    docker rm ceramic-pg --force 2> /dev/null
    docker run --name ceramic-pg -e POSTGRES_DB=ceramic -e POSTGRES_PASSWORD=c3ram1c -p 5432:5432 -d postgres:16 
    absolute_db_path="postgresql://postgres:c3ram1c@localhost:5432/ceramic"
    absolute_migrations="$(pwd)/migrations/postgres"
    prepare_database $absolute_db_path $absolute_migrations
    if [ "$CI_RUN" ]; then
        docker stop ceramic-pg
        docker rm ceramic-pg
    fi
    ;;
  *)
    echo "Using sqlite"
    absolute_db_path="$(pwd)/ceramic_cicddb.sqlite"
    absolute_migrations="$(pwd)/migrations/sqlite"
    prepare_database "sqlite://$absolute_db_path" $absolute_migrations
    if [ "$CI_RUN" ]; then
        rm $absolute_db_path
    fi
    ;;
esac

