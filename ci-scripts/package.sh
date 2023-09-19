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
OUT_FILE=ceramic-one.$EXT
OUT_PATH=$ARTIFACTS_DIR/ceramic-one.$EXT
BIN_DIR=target/$TARGET/release

echo "Determining package version"
PKG_VERSION=$(cargo metadata --format-version=1 --no-deps | jq '.packages[0].version' | tr -d '"')

if [ ! -d $ARTIFACTS_DIR ]; then
  mkdir -p $ARTIFACTS_DIR
fi
if [ -f $OUT_PATH ]; then
  rm $OUT_PATH
fi

echo "Building artifacts for "$TARGET

cargo build --release --locked --target $TARGET

echo "Building package for "$TARGET
fpm --fpm-options-file $CONFIG_FILE -C $BIN_DIR -v $PKG_VERSION -p $OUT_PATH ceramic-one=$INSTALL_DIR/ceramic-one

echo "Compressing package for "$TARGET
tar -cvzf ceramic-one_$TARGET.tar.gz -C $ARTIFACTS_DIR $OUT_FILE
