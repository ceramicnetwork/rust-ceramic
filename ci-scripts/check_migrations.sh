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

USE_PG=0
USE_SQLITE=0

check_db_type() {
    case $1 in
    postgres*)
        USE_PG=1
        ;;
    sqlite*)
        USE_SQLITE=1
        ;;
    *) ;;
    esac
}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
cd "$SCRIPT_DIR/.."
PROJECT_DIR=$(pwd)

absolute_sqlite_path="$PROJECT_DIR/ceramic_cicddb.sqlite"
absolute_sqlite_migrations="$PROJECT_DIR/migrations/sqlite"

if [ -n "$TEST_DATABASE_URL" ]; then
    absolute_pg_path=$TEST_DATABASE_URL
else
    absolute_pg_path="postgresql://postgres:c3ram1c@localhost:5432/ceramic_one_tests"
fi
absolute_pg_migrations="$PROJECT_DIR/migrations/postgres"

if [ -z "$1" ]; then
    echo "Parameter required. Should be 'sqlite' or 'postgres'"
    exit 1
fi

cargo install sqlx-cli

# Check the first input parameter
if [ -n "$1" ]; then
    check_db_type "$1"
    if [ -n "$2" ]; then
        check_db_type "$2"
    fi
else
    echo "Must specify 'sqlite' or 'postgres'"
    exit 1
fi

if [ -n "$MIGRATE_DB" ]; then
    if (($USE_SQLITE)); then
        echo "Using sqlite"
        prepare_database "sqlite://$absolute_sqlite_path" "$absolute_sqlite_migrations"
    fi

    if (($USE_PG)); then
        echo "Using postgres"
        if [ -z "$TEST_DATABASE_URL" ]; then
            docker rm ceramic-pg --force 2>/dev/null
            docker run --name ceramic-pg -e POSTGRES_DB=ceramic_one_tests -e POSTGRES_PASSWORD=c3ram1c -p 5432:5432 -d postgres:16
            sleep 2
        fi
        prepare_database "$absolute_pg_path" "$absolute_pg_migrations"
    fi
fi

if [ -n "$RUN_DB_TESTS" ]; then
    echo "Running tests"
    PG_TESTS=1 RUSTFLAGS="-D warnings --cfg tokio_unstable" cargo test -p ceramic-store --locked --release
fi

if [ -n "$MIGRATION_CLEANUP" ]; then
    if (($USE_SQLITE)); then
        echo "Cleaning up sqlite"
        if -w "$absolute_sqlite_path"; then
            rm $absolute_sqlite_path
        else 
            echo "Cannot delete $absolute_sqlite_path (non-existent or not writable)"
        fi
    fi

    if (($USE_PG)); then
        running=$(docker ps -f "name=ceramic-pg" -q)
        if [ -n "$running" ]; then
            echo "Cleaning up postgres"
            docker stop ceramic-pg
            docker rm ceramic-pg
        else 
            echo "No running postgres container found"
        fi
    fi
fi
