#!/usr/bin/env bash
# Check specified release level based on commit messages, then if appropriate, release package
VALID_ARGS=$(getopt -o l: --long level: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

LEVEL=minor

eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -l | --level)
        LEVEL=echo "$2" | tr '[:upper:]' '[:lower:]'
        case "$LEVEL" in
            major|minor|patch) ;;
            *)
                echo "Invalid release level: $LEVEL"
                exit 1
                ;;
        esac
        shift 2
        ;;
    --) shift;
        break
        ;;
  esac
done

cd $(git rev-parse --show-toplevel)

# Using git cliff determine if there are any feat commits that do not belong to a tag.
git cliff --unreleased --strip all --body "{% for group, commits in commits | group_by(attribute=\"group\") %}
{{ group }} {{ commits | length }}
{% endfor %}" | grep Features
ret=$?
if [[ $ret = 0 ]] && [[ $LEVEL = patch ]]; then
    echo "Cannot release patch version when there are unreleased features."
    exit 1
fi

git config user.email "github@3box.io"
git config user.name "Github Automation"
TAG=$(cargo metadata --format-version=1 --no-deps | jq '.packages[0].version' | tr -d '"')
cargo release -vv $LEVEL --exclude ceramic-api-server --exclude ceramic-kubo-rpc-server --no-confirm

#gh release create v$TAG --title "v"$TAG --latest artifacts/**/*