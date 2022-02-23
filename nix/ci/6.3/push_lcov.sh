#!/usr/bin/env bash

set -e
set -u
set -o pipefail

cp /opt/fdb/conf/fdb.cluster /home/runner/fdb.cluster

echo "+-------------------------------+"
echo "| Generate Code Coverage Report |"
echo "+-------------------------------+"

cd ../../fdb || { echo "cd failure"; exit 1; }

mkdir -p lcov

# Run unit tests and integration tests

cargo llvm-cov --lib --tests --features=fdb-6_3 --lcov --output-path lcov/tests.info

# Run examples

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_committed_version --features=fdb-6_3 --lcov --output-path lcov/get_committed_version.info

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_range --features=fdb-6_3 --lcov --output-path lcov/get_range.info

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_versionstamp --features=fdb-6_3 --lcov --output-path lcov/get_versionstamp.info

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example hello_world --features=fdb-6_3 --lcov --output-path lcov/hello_world.info

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example open_database --features=fdb-6_3 --lcov --output-path lcov/open_database.info

/opt/fdb/cli/6.3.23/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example watch --features=fdb-6_3 --lcov --output-path lcov/watch.info

lcov --add-tracefile lcov/tests.info --add-tracefile lcov/get_committed_version.info --add-tracefile lcov/get_range.info --add-tracefile lcov/get_versionstamp.info --add-tracefile lcov/hello_world.info --add-tracefile lcov/open_database.info --add-tracefile lcov/watch.info --output-file ../lcov.info
