#!/usr/bin/env bash

set -e
set -u
set -o pipefail

echo "+---------------------+"
echo "| Build documentation |"
echo "+---------------------+"

cd ../../ || { echo "cd failure"; exit 1; }

cd fdb-gen || { echo "cd failure"; exit 1; }

cargo doc --lib --no-deps --features=fdb-7_1

cd ../ || { echo "cd failure"; exit 1; }

cd fdb-sys || { echo "cd failure"; exit 1; }

cargo doc --lib --no-deps --features=fdb-7_1

cd ../ || { echo "cd failure"; exit 1; }

cd fdb || { echo "cd failure"; exit 1; }

cargo doc --lib --no-deps --features=fdb-7_1

cd ../ || { echo "cd failure"; exit 1; }

