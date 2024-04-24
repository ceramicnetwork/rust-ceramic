#!/usr/bin/env bash
set -e

# Script to generate the offline sqlx query data
# Will prompt user if CI_RUN is not set
# Will run tests if RUN_DB_TESTS is set
# Will cleanup the db (e.g. stop docker and delete file) if MIGRATION_CLEANUP is set

function prepare_database() {
    echo "Resetting database at $1"
    if [ -z "$CI_RUN" ]; then
        cargo sqlx database reset --database-url "$1"
    else
        cargo sqlx database reset --database-url "$1" -y
    fi
    echo "Applying migrations at $2"
    cargo sqlx database setup --database-url "$1" --source "$2"
    # we no longer support query! since we have multiple sql backends
    # but we leave it behind a variable in case we want to run it someday
    if [ -n "$QUERY_MACROS" ]; then
        cd "$PROJECT_DIR/store"
        if [ -z "$CI_RUN" ]; then
            cargo sqlx prepare --database-url "$1"
        else
            cargo sqlx prepare --database-url "$1" --check
            cd $PROJECT_DIR
        fi
    fi
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR/.."
PROJECT_DIR=$(pwd)

if [ -n "$TEST_DATABASE_URL" ]; then
    absolute_sqlite_path=$TEST_DATABASE_URL
else
    absolute_sqlite_path="$PROJECT_DIR/ceramic_cicddb.sqlite"
fi

absolute_sqlite_migrations="$PROJECT_DIR/migrations/sqlite"

cargo install sqlx-cli

if [ -n "$MIGRATE_DB" ]; then
    echo "Using sqlite"
    prepare_database "sqlite://$absolute_sqlite_path" "$absolute_sqlite_migrations"

fi

if [ -n "$RUN_DB_TESTS" ]; then
    echo "Running tests"
    RUSTFLAGS="-D warnings --cfg tokio_unstable" cargo test -p ceramic-store --locked --release
fi

if [ -n "$MIGRATION_CLEANUP" ]; then
    echo "Cleaning up sqlite"
    if -w "$absolute_sqlite_path"; then
        rm $absolute_sqlite_path
    else
        echo "Cannot delete $absolute_sqlite_path (non-existent or not writable)"
    fi

fi
