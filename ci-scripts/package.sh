#!/usr/bin/env bash
# Script to package our application for distribution.
VALID_ARGS=$(getopt -o fedia: --long config-file,extension,binary-dir,install-dir,architecture: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

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

eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -f | --config-file)
        CONFIG_FILE=$2
        shift 2
        ;;
    -e | --extension)
        EXT=$2
        shift 2
        ;;
    -d | --binary-dir)
        BIN_DIR=$2
        shift 2
        ;;
    -i | --install-dir)
        INSTALL_DIR=$2
        shift 2
        ;;
    -a | --architecture)
        ARCH=$2
        shift 2
        ;;
    --) shift;
        break
        ;;
  esac
done

ARTIFACTS_DIR=artifacts
OUT_FILE=$ARTIFACTS_DIR/ceramic-one.$EXT
PKG_VERSION=$(cargo metadata --format-version=1 --no-deps | jq '.packages[0].version' | tr -d '"')
BIN_DIR=target/$TARGET/release

if [ -f $OUT_FILE ]; then
  rm $OUT_FILE
fi

echo "Building package for "$TARGET

cargo build --release --target $TARGET

mkdir $ARTIFACTS_DIR || true

fpm --fpm-options-file $CONFIG_FILE -C $BIN_DIR -v $PKG_VERSION -p $OUT_FILE ceramic-one=$INSTALL_DIR/ceramic-one
