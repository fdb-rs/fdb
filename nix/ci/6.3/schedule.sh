#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cp /opt/fdb/conf/fdb.cluster /home/runner/fdb.cluster

echo ""
echo "+------------------------------+"
echo "| Setup and run binding tester |"
echo "+------------------------------+"

cd ../../fdb-stacktester/fdb-stacktester-630 || { echo "cd failure"; exit 1; }

cargo build --bin fdb-stacktester-630 --release

pip install foundationdb==6.3.24

# Run `scripted` test once. This is similar to how it is done in
# `run_tester_loop.sh`.
./bindingtester/bindingtester/bindingtester.py rust --test-name scripted --logging-level WARNING

# Adjust the number of iterations so it takes approximately an hour to
# finish the run.

START=1
END=400

for i in $(eval echo "{$START..$END}")
do
    echo "Running interation $i"

    ./bindingtester/bindingtester/bindingtester.py rust --test-name api --compare --num-ops 1000 --logging-level WARNING

    ./bindingtester/bindingtester/bindingtester.py rust --test-name api --num-ops 1000 --concurrency 5 --logging-level WARNING
done
