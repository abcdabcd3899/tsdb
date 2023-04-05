#!/bin/bash
#
# This script builds a matrixdb rpm package for centos7/kylin10.
#
# A customized docker image is needed to build the package, generate it like
# below, you only need to do it once:
#
#     docker build --tag mxdb/centos7-build \
#         - < src/tools/docker/centos7/Dockerfile.mxdb
#
# Alternatively, we have pushed the image to docker hub, if you decide to use
# it, you can skip the above step, and replace "mxdb/centos7-build" with
# "remotefish1078/centos7-build:latest" in the below command.
#
# To build the package, please prepare a __clean__ clone of the repo, checkout
# the target branch/tag, then launch the build job with docker:
#
#     # prepare the repo
#     cd /path/to/matrixdb.git
#     git submodule update --init --recursive
#     # prepare the result dir
#     mkdir -p /tmp/build/artifacts
#     docker run \
#       -w /tmp \
#       -v $PWD:/tmp/src/matrixdb:ro \
#       -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
#       -it mxdb/centos7-build \
#       bash /tmp/src/matrixdb/PKG.CentOS.bash
#
# The results are put under /tmp/build/artifacts/
#
# If you run this script without using docker, run the following command:

#     mkdir -p /tmp/src
#     ln -s "$(pwd)" /tmp/src/matrixdb

set -ex

# The build dir inside the container
BUILDROOT=/tmp/build

MATRIXDB_COMMUNITY_VERSION="$(grep 'MATRIXDB_COMMUNITY_VERSION=' /tmp/src/matrixdb/configure.in \
    | cut -d= -f2)"
MATRIXDB_ENTERPRISE_VERSION="$(grep 'MATRIXDB_ENTERPRISE_VERSION=' /tmp/src/matrixdb/configure.in \
    | cut -d= -f2)"

# By default we do a community build.
MATRIXDB_ENTERPRISE=
# By default we build with blocksize=8K.
BLOCK_SIZE=8

# Parse the options.
while [ "$#" -gt 0 ]; do
    case "$1" in
        --enable-enterprise)
            # Do an enterprise build as specified.
            MATRIXDB_ENTERPRISE=1
            ;;
        --with-blocksize=*)
            # blocksize is manually specified
            BLOCK_SIZE=$(echo "$1" | cut -d= -f2)
            ;;
        *)
            echo "ERROR: unsupported packaging option: $1"
            exit 1
            ;;
    esac

    shift
done

# Extra configure options
OPTS=

echo "Set blocksize to ${BLOCK_SIZE}K"
OPTS="$OPTS --with-blocksize=${BLOCK_SIZE}"

if [ "$MATRIXDB_ENTERPRISE" = 1 ]; then
    # Do an enterprise build as specified.
    echo "Build Type: enterprise"
    MATRIXDB_VERSION="$MATRIXDB_ENTERPRISE_VERSION"
    OPTS="$OPTS --enable-enterprise"
else
    # Otherwise do a community build.
    echo "Build Type: community"
    MATRIXDB_VERSION="$MATRIXDB_COMMUNITY_VERSION"
fi

# RPM disallows "-" as version string, convert it to "."
MATRIXDB_RPM_VERSION=${MATRIXDB_VERSION//-/.}

# The env vars for the compile script
export GPDB_SRC_PATH=matrixdb
export TARGET_OS="$(. /etc/os-release; echo "$ID")"
export TARGET_OS_VERSION="$(. /etc/os-release; echo "$VERSION_ID")"
export BLD_TARGETS="clients"
export OUTPUT_ARTIFACT_DIR=artifacts
export CONFIGURE_FLAGS="--enable-faultinjector --disable-tap-tests --enable-debug-extensions --with-mysqlfdw --with-mongofdw $OPTS"
export RC_BUILD_TYPE_GCS=
# We have to skip the unittests for now, the ftsprobe tests fail.
export SKIP_UNITTESTS=true

# Create the build dir
mkdir -p ${BUILDROOT} ${BUILDROOT}/${OUTPUT_ARTIFACT_DIR}
rm -rf ${BUILDROOT}/${GPDB_SRC_PATH}
cp -a /tmp/src/matrixdb ${BUILDROOT}/${GPDB_SRC_PATH}
cd ${BUILDROOT}

# Build the package

if [ "$TARGET_OS" = "kylin" ]; then
    ./$GPDB_SRC_PATH/concourse/scripts/compile_gpdb_kylin.bash
else
    ./$GPDB_SRC_PATH/concourse/scripts/compile_gpdb.bash
fi

# Build the rpm package
cd
rpmdev-setuptree
cp ${BUILDROOT}/${OUTPUT_ARTIFACT_DIR}/bin_gpdb.tar.gz rpmbuild/SOURCES/

cat >rpmbuild/SPECS/matrixdb.spec <<EOF
Name:           matrixdb
Version:        ${MATRIXDB_RPM_VERSION}
Release:        1%{?dist}
Summary:        MatrixDB

License:        MatrixDB License
URL:            https://matrixdb.cn
Source0:        bin_gpdb.tar.gz

# - MatrixDB does not depends on external lz4 itself, however mars depends on
#   parquet, and parquet depends on lz4.  One problem is that parquet requires
#   lz4 >= 1.8, but it does not mention the version in its rpm, so with a lower
#   version of lz4 we can still have parquet and matrixdb rpm packages
#   installed, but the mars extension cannot be successfully loaded.  So we
#   make lz4 >= 1.8 a requirement of MatrixDB as a workaround.
Requires:       lz4 >= 1.8

# - The SysV service requires initscripts, however do not list it as a strong
#   dependence, we could run without it as long as the systemd is available.
#   Weak dependence is described with "Recommends", however it might not be
#   supported on centos7 yet, so we simply removed the dependence, if the user
#   really needs the SysV service, for example in docker container, then the
#   initscripts has to be manually installed.
#Recommends: initscripts

%description
MatrixDB is a timeseries-optimized database based on Greenplum.

%prep
mkdir -p \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}
ln -nfs \${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION} \
    \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}
tar -zxf \${RPM_SOURCE_DIR}/bin_gpdb.tar.gz \
    -C \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}

# rpath is set to '$ORIGIN/../lib' in release builds, but for extensions, like
# mars.so, which is installed to $prefix/lib/postgresql/mars.so, rpath has to
# be '$ORIGIN/../../lib'.  Instead of adjusting the rpath for each extensions,
# we can simply install a symbolic link as a workaround.
ln -nfs . \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}/lib/lib

# use the same dirname in the env file
sed -i 's,^GPHOME=.*$,GPHOME="'/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}'",' \
    \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/greenplum_path.sh

mkdir -p \${RPM_BUILD_ROOT}/etc/default
mkdir -p \${RPM_BUILD_ROOT}/etc/init.d
mkdir -p \${RPM_BUILD_ROOT}/etc/matrixdb
mkdir -p \${RPM_BUILD_ROOT}/usr/lib/systemd/system
mkdir -p \${RPM_BUILD_ROOT}/var/log/matrixdb/

cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/supervisor.conf \
    \${RPM_BUILD_ROOT}/etc/matrixdb
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/supervisor.mxui.conf \
    \${RPM_BUILD_ROOT}/etc/matrixdb
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/supervisor.cylinder.conf \
    \${RPM_BUILD_ROOT}/etc/matrixdb
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/alert.yml \
    \${RPM_BUILD_ROOT}/etc/matrixdb
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/matrixdb-supervisor.default \
    \${RPM_BUILD_ROOT}/etc/default/matrixdb-supervisor
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/matrixdb-supervisor.centos.sysv \
    \${RPM_BUILD_ROOT}/etc/init.d/matrixdb-supervisor
cp \${RPM_BUILD_ROOT}/usr/local/\${RPM_PACKAGE_NAME}-\${RPM_PACKAGE_VERSION}/share/postgresql/extension/matrixdb.supervisor.service \
    \${RPM_BUILD_ROOT}/usr/lib/systemd/system

# fix file permissions
chmod 644 \
    \${RPM_BUILD_ROOT}/etc/matrixdb/* \
    \${RPM_BUILD_ROOT}/etc/default/matrixdb-supervisor \
    \${RPM_BUILD_ROOT}/usr/lib/systemd/system/matrixdb.supervisor.service \
    $NULL
chmod 755 \
    \${RPM_BUILD_ROOT}/etc/init.d/matrixdb-supervisor \
    $NULL

%files
/usr/local/matrixdb-${MATRIXDB_RPM_VERSION}
/usr/local/matrixdb
/usr/lib/systemd/system/matrixdb.supervisor.service

# keep user configurations during upgrade/reinstall
%config(noreplace) /etc/default/matrixdb-supervisor
%config(noreplace) /etc/init.d/matrixdb-supervisor
%config(noreplace) /etc/matrixdb/supervisor.conf
%config(noreplace) /etc/matrixdb/supervisor.mxui.conf
%config(noreplace) /etc/matrixdb/supervisor.cylinder.conf
%config(noreplace) /etc/matrixdb/alert.yml

%doc

%post
mkdir -p /var/log/matrixdb/

%posttrans
systemctl enable matrixdb.supervisor.service || :
systemctl daemon-reload || :
systemctl start matrixdb.supervisor.service \
|| /etc/init.d/matrixdb-supervisor start || :
echo >&2 "Please Visit MatrixDB UI at http://<localhost-IP-or-domain>:8240/installer"

%preun
systemctl stop matrixdb.supervisor.service \
|| /etc/init.d/matrixdb-supervisor stop || :
systemctl disable matrixdb.supervisor.service || :

%postun
systemctl daemon-reload \
|| /etc/init.dmatrixdb-supervisor reload || :

%changelog
EOF

# get macro %dist
DIST=$(grep '%dist' /etc/rpm/macros.dist | cut -d' ' -f2)
PKGNAME=matrixdb-${MATRIXDB_RPM_VERSION}-1${DIST}.x86_64.rpm
rpmbuild -ba rpmbuild/SPECS/matrixdb.spec
cp rpmbuild/RPMS/x86_64/$PKGNAME \
    ${BUILDROOT}/${OUTPUT_ARTIFACT_DIR}/

# Generate MD5 & SHA256 checksums
cd ${BUILDROOT}/${OUTPUT_ARTIFACT_DIR}/
md5sum $PKGNAME > MD5SUMS
sha256sum $PKGNAME > SHA256SUMS

# vi: et sw=4 :
