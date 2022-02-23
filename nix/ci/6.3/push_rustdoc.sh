#!/usr/bin/env bash

set -e
set -u
set -o pipefail

echo "+---------------------+"
echo "| Build documentation |"
echo "+---------------------+"

cd ../../ || { echo "cd failure"; exit 1; }

cargo doc --all --no-deps --features=fdb-6_3
