#!/usr/bin/env bash
#
# This script is a small wrapper to download the jar file for openapi-generator-cli and run it.

VERSION='7.2.0'
jar=openapi-generator-cli.jar

if [ ! -f $jar ]
then
    curl https://repo1.maven.org/maven2/org/openapitools/openapi-generator-cli/${VERSION}/openapi-generator-cli-${VERSION}.jar -o $jar
fi

java -ea                          \
  -Xms512M                        \
  -Xmx1024M                       \
  -server                         \
  -jar ./${jar} "$@"
