#!/usr/bin/env bash
# Script to package our application for distribution.

echo "Preparing to package application"

EXT="deb"
PKG_TYPE="dpkg"
CONFIG_FILE="fpm/linux.fpm"
INSTALL_DIR="/usr/local/bin"
TARGET="x86_64-unknown-linux-gnu"

ARCH=""
case $(uname -m) in
  x86_64) ARCH="x86_64" ;;
  arm64) ARCH="aarch64" ;;
  --)
    echo "Unknown architecture $(uname -m)"
    exit 1
    ;;
esac

if [[ "$OSTYPE" == "darwin"* ]]; then
  CONFIG_FILE="fpm/osx.fpm"
  EXT="pkg"
  PKG_TYPE="osxpkg"
  INSTALL_DIR="/Applications"
  TARGET=$ARCH"-apple-darwin"
fi

echo "Evaluating program arguments '$@'"

while getopts "f:e:d:i:a:" opt
do
  case "$opt" in
    f)
      echo "Setting config file to "$OPTARG
      CONFIG_FILE=$OPTARG
      ;;
    e)
      echo "Setting extension to "$OPTARG
      EXT=$OPTARG
      ;;
    d)
      echo "Setting bin dir to "$OPTARG
      BIN_DIR=$OPTARG
      ;;
    i)
      echo "Setting install dir to "$OPTARG
      INSTALL_DIR=$OPTARG
      ;;
    a)
      echo "Setting architecture to "$OPTARG
      ARCH=$OPTARG
      ;;
    \? )
      echo "Invalid option: -$OPTARG" 1>&2
      exit 1
      ;;
    : )
      echo "Invalid option: -$OPTARG requires an argument" 1>&2
      ;;
  esac
done

ARTIFACTS_DIR=artifacts
OUT_FILE=$ARTIFACTS_DIR/ceramic-one.$EXT
BIN_DIR=target/$TARGET/release

echo "Determining package version"
PKG_VERSION=$(cargo metadata --format-version=1 --no-deps | jq '.packages[0].version' | tr -d '"')

if [ -f $OUT_FILE ]; then
  rm $OUT_FILE
fi

echo "Building artifacts for "$TARGET

cargo build --release --locked --target $TARGET

mkdir $ARTIFACTS_DIR || true

echo "Building package for "$TARGET

fpm --fpm-options-file $CONFIG_FILE -C $BIN_DIR -v $PKG_VERSION -p $OUT_FILE ceramic-one=$INSTALL_DIR/ceramic-one
