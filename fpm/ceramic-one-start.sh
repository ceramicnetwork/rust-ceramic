#!/usr/bin/env bash

SQITCH_CMD=$(which sqitch)
if [ -z "$SQITCH_CMD" ]; then
  echo "Sqitch not found, please install"
  exit 1
fi

sqitch verify db:sqlite:db.sqlite3

./ceramic-one