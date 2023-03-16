#!/bin/bash
# Script to determine the next release level based on conventional commits.
#
# Possible levels:
# * patch
# * minor
#
# Any feature commits indicates a minor release. The lack of feature commits indicates
# a patch release. Major releases are expected to be handled manually.
#
# Assumptions:
# * git is installed
# * git-cliff is installed
# * grep is installed

# Ensure we are in the git root
cd $(git rev-parse --show-toplevel)

level=patch

# Using git cliff determine if there are any feat commits that do not belong to a tag.
git cliff --unreleased --strip all --body "{% for group, commits in commits | group_by(attribute=\"group\") %}
{{ group }} {{ commits | length }}
{% endfor %}" | grep Features
ret=$?
if [[ $ret = 0 ]]
then
    level=minor
fi

echo $level
