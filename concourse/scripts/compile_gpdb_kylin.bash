#!/bin/bash

GREENPLUM_INSTALL_DIR=/usr/local/matrixdb_dev
ARTIFACT_DIR=$(pwd)/${OUTPUT_ARTIFACT_DIR}
rm -rf ${GREENPLUM_INSTALL_DIR}
mkdir -p ${GREENPLUM_INSTALL_DIR}
cd ${GPDB_SRC_PATH}
./configure --with-perl --with-python --with-libxml --with-gssapi --prefix=${GREENPLUM_INSTALL_DIR}
make -j4
make -j4 install

# copy from compile_gpdb.bash
pushd ${GREENPLUM_INSTALL_DIR}
	source greenplum_path.sh
	python3 -m compileall -q -x test .
	chmod -R 755 .
	tar -czf "${ARTIFACT_DIR}/bin_gpdb.tar.gz" ./*
popd
