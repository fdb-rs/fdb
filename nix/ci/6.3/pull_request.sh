#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cp /opt/fdb/conf/fdb.cluster /home/runner/fdb.cluster

echo "+--------------------------+"
echo "| Build and test fdb crate |"
echo "+--------------------------+"

cd ../../fdb || { echo "cd failure"; exit 1; }

cargo build --lib --features=fdb-6_3

cargo build --examples --features=fdb-6_3

RUSTDOCFLAGS="--deny warnings" cargo doc --lib --features=fdb-6_3

cargo test --features=fdb-6_3

echo ""
echo "+-------------------------------------------+"
echo "| Check workspace formatting and run clippy |"
echo "+-------------------------------------------+"

cd ../ || { echo "cd failure"; exit 1; }

cargo fmt --all --check

cargo clippy --workspace --lib --bins --examples --tests --features=fdb-6_3 -- --deny warnings

echo ""
echo "+------------------------------+"
echo "| Setup and run binding tester |"
echo "+------------------------------+"

cd fdb-stacktester/fdb-stacktester-630 || { echo "cd failure"; exit 1; }

cargo build --bin fdb-stacktester-630 --release

pip install foundationdb==6.3.23

# Run `scripted` test once. This is similar to how it is done in
# `run_tester_loop.sh`.
./bindingtester/bindingtester/bindingtester.py rust --test-name scripted --logging-level WARNING

# Some test with binding tester with 10 iterations. Exhaustive testing
# happens using cron.

START=1
END=10

for i in $(eval echo "{$START..$END}")
do
    echo "Running interation $i"

    ./bindingtester/bindingtester/bindingtester.py rust --test-name api --compare --num-ops 1000 --logging-level WARNING

    ./bindingtester/bindingtester/bindingtester.py rust --test-name api --num-ops 1000 --concurrency 5 --logging-level WARNING
done
