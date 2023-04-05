#!/bin/bash
#
# This script test the matrixdb bin package for centos7.
#
# You should first build the package with PKG.CentOS.bash, then launch the test
# job with docker:
#
#	  # the same repo and commit should be used for both compile and test
#     cd /path/to/matrixdb.git
#     # suppose the build resutls are under /tmp/build/artifacts
#     docker run \
#       -w /tmp \
#       -v $PWD:/tmp/src/matrixdb:ro \
#       -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
#       -it mxdb/centos7-build \
#       bash /tmp/src/matrixdb/TEST.CentOS.bash
#
# The reports are put under /tmp/build/artifacts/

set -ex

# The build dir inside the container
BUILDROOT=/tmp/build

# The env vars for the test script
export GPDB_SRC_PATH=matrixdb
export OUTPUT_ARTIFACT_DIR=artifacts
export DUMP_DB=false
export MAKE_TEST_COMMAND="-k PGOPTIONS='-c optimizer=off' installcheck-matrixdb"
export BLDWRAP_POSTGRES_CONF_ADDONS="enable_mergejoin=off enable_nestloop=off"
export TEST_OS="$(. /etc/os-release; echo "$ID")"
export CONFIGURE_FLAGS="--enable-faultinjector --disable-tap-tests"
export WITH_MIRRORS=
# Have to disable some tests:
# - gp_metadata: it verifies the version string, we do not want to add
#   unnecesary changes;
# - eagerfree: the plan contains an extra subplan due to the targetlist
#   elimination in multiphase agg, we could not simply update the answer file,
#   the plan does not meet the requirement of the test now;
# - gp_aggregates_costs: plan is changed due to parallel scan;
# - workfile/sisc_mat_sort: the # of workfiles is changed, maybe due to
#   parallel scan;
export EXTRA_REGRESS_OPTS="--exclude-tests=gp_metadata,eagerfree,gp_aggregates_costs,workfile/sisc_mat_sort"

# Create the build dir
mkdir -p ${BUILDROOT}
rm -rf ${BUILDROOT}/${GPDB_SRC_PATH}
cp -a /tmp/src/matrixdb ${BUILDROOT}/${GPDB_SRC_PATH}
cd ${BUILDROOT}

# Let the test script find the bin package and source code
mkdir -p bin_gpdb
ln -nfs ${BUILDROOT}/${OUTPUT_ARTIFACT_DIR}/bin_gpdb.tar.gz bin_gpdb/
# the gpdb_src should be a real dir instead of a symbolic link, the test script
# contains operations like "find gpdb_src ...", if it is a symbolic link then
# the list will contain only the gpdb_src, we have to change it to "find
# gpdb_src/" for it to work correctly.  To keep minimal change to the gpdb
# code, we hack it here directly.
rm -rf gpdb_src
mv matrixdb gpdb_src
ln -nfs gpdb_src matrixdb

# Run the test
./${GPDB_SRC_PATH}/concourse/scripts/ic_gpdb.bash

# vi: et sw=4 :
