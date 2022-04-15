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

cargo llvm-cov --lib --tests --features=fdb-7_1 --lcov --output-path lcov/tests.info

# Run examples

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_committed_version --features=fdb-7_1 --lcov --output-path lcov/get_committed_version.info

# # There seems to be a bug [1] that is causing this `run` to fail. Add
# # it back once the bug is resolved.
# #
# # [1] https://forums.foundationdb.org/t/everything-about-getmappedrange/3280/3

# /opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

# cargo llvm-cov run --example get_mapped_range --features=fdb-7_1 --lcov --output-path lcov/get_mapped_range.info

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_range --features=fdb-7_1 --lcov --output-path lcov/get_range.info

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example get_versionstamp --features=fdb-7_1 --lcov --output-path lcov/get_versionstamp.info

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example hello_world --features=fdb-7_1 --lcov --output-path lcov/hello_world.info

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example open_database --features=fdb-7_1 --lcov --output-path lcov/open_database.info

/opt/fdb/cli/7.1.3/fdbcli -C /home/runner/fdb.cluster --exec "writemode on; clearrange \x00 \xff"

cargo llvm-cov run --example watch --features=fdb-7_1 --lcov --output-path lcov/watch.info

# Need to add `--add-tracefile lcov/get_mapped_range.info` once the
# bug mentioned above is resolved.

lcov --add-tracefile lcov/tests.info --add-tracefile lcov/get_committed_version.info --add-tracefile lcov/get_range.info --add-tracefile lcov/get_versionstamp.info --add-tracefile lcov/hello_world.info --add-tracefile lcov/open_database.info --add-tracefile lcov/watch.info --output-file ../lcov.info
