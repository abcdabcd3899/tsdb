#!/bin/bash
set -exo pipefail

GREENPLUM_INSTALL_DIR=/usr/local/greenplum-db-devel
export GPDB_ARTIFACTS_DIR=$(pwd)/${OUTPUT_ARTIFACT_DIR}
CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GPDB_SRC_PATH=${GPDB_SRC_PATH:=gpdb_src}

GPDB_BIN_FILENAME=${GPDB_BIN_FILENAME:="bin_gpdb.tar.gz"}
GREENPLUM_CL_INSTALL_DIR=/usr/local/greenplum-clients-devel
GPDB_CL_FILENAME=${GPDB_CL_FILENAME:="gpdb-clients-${TARGET_OS}${TARGET_OS_VERSION}.tar.gz"}

function expand_glob_ensure_exists() {
  local -a glob=($*)
  [ -e "${glob[0]}" ]
  echo "${glob[0]}"
}

function prep_env() {
  case "${TARGET_OS}" in
  centos)
    case "${TARGET_OS_VERSION}" in
    6 | 7) BLD_ARCH=rhel${TARGET_OS_VERSION}_x86_64 ;;
    *)
      echo "TARGET_OS_VERSION not set or recognized for Centos/RHEL"
      exit 1
      ;;
    esac
    ;;
  ubuntu)
    case "${TARGET_OS_VERSION}" in
    18.04) BLD_ARCH=ubuntu18.04_x86_64 ;;
    *)
      echo "TARGET_OS_VERSION not set or recognized for Ubuntu"
      exit 1
      ;;
    esac
    ;;
  sles)
    case "${TARGET_OS_VERSION}" in
    12) export BLD_ARCH=sles12_x86_64 ;;
    *)
      echo "TARGET_OS_VERSION not set or recognized for SLES"
      exit 1
      ;;
    esac
    ;;
  esac
}

function install_libuv() {
  local includedir=/usr/include
  local libdir

  case "${TARGET_OS}" in
    centos | sles) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/lib/x86_64-linux-gnu ;;
    *) return ;;
  esac
  # provided by build container
  cp -a /usr/local/include/uv* ${includedir}/
  cp -a /usr/local/lib*/libuv* ${libdir}/
}

function install_deps_for_centos_or_sles() {
  # quicklz is disabled for matrixdb
  return 0
  rpm -i libquicklz-installer/libquicklz-*.rpm
  rpm -i libquicklz-devel-installer/libquicklz-*.rpm
}

function install_deps_for_ubuntu() {
  # quicklz is disabled for matrixdb
  return 0
  dpkg --install libquicklz-installer/libquicklz-*.deb
}

function install_deps() {
  case "${TARGET_OS}" in
    centos | sles) install_deps_for_centos_or_sles;;
    ubuntu) install_deps_for_ubuntu;;
  esac
  install_libuv
}

function generate_build_number() {
  pushd ${GPDB_SRC_PATH}
    #Only if its git repro, add commit SHA as build number
    # BUILD_NUMBER file is used by getversion file in GPDB to append to version
    if [ -d .git ] ; then
      echo "commit:`git rev-parse HEAD`" > BUILD_NUMBER
    fi
  popd
}

function build_gpdb() {
  pushd ${GPDB_SRC_PATH}/gpAux
    # Use -j4 to speed up the build. (Doesn't seem worth trying to guess a better
    # value based on number of CPUs or anything like that. Going above -j4 wouldn't
    # make it much faster, and -j4 is small enough to not hurt too badly even on
    # a single-CPU system
    if [ -n "$1" ]; then
      make "$1" GPROOT=/usr/local PARALLEL_MAKE_OPTS=-j4 -s dist
    else
      make GPROOT=/usr/local PARALLEL_MAKE_OPTS=-j4 -s dist
    fi
  popd
}

# MatrixDB specific build steps
function build_mxdb() {
  # need to build mars with the same g++ version used by apache parquet, which
  # is the system default one, 4.8 on centos7.
  if [ -d ${GPDB_SRC_PATH}/contrib/mars ]; then
    make clean -C ${GPDB_SRC_PATH}/contrib/mars
    make -C ${GPDB_SRC_PATH}/contrib/mars -j4 \
      CXX=/usr/bin/g++ \
      install
  fi
}

function git_info() {
  pushd ${GPDB_SRC_PATH}

  "${CWDIR}/git_info.bash" | tee ${GREENPLUM_INSTALL_DIR}/etc/git-info.json

  PREV_TAG=$(git describe --tags --abbrev=0 HEAD^)

  cat > ${GREENPLUM_INSTALL_DIR}/etc/git-current-changelog.txt <<-EOF
	======================================================================
	Git log since previous release tag (${PREV_TAG})
	----------------------------------------------------------------------

	EOF

  git log --abbrev-commit --date=relative "${PREV_TAG}..HEAD" >> ${GREENPLUM_INSTALL_DIR}/etc/git-current-changelog.txt

  popd
}

function unittest_check_gpdb() {
  pushd ${GPDB_SRC_PATH}
    source ${GREENPLUM_INSTALL_DIR}/greenplum_path.sh
    make GPROOT=/usr/local -s unittest-check
  popd
}

function include_zstd() {
  local libdir
  case "${TARGET_OS}" in
    centos | sles) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/lib ;;
    *) return ;;
  esac
  pushd ${GREENPLUM_INSTALL_DIR}
    cp ${libdir}/pkgconfig/libzstd.pc lib/pkgconfig
    cp -d ${libdir}/libzstd.so* lib
    cp /usr/include/zstd*.h include
  popd
}

function include_quicklz() {
  # quicklz is disabled for matrixdb
  return 0
  local libdir
  case "${TARGET_OS}" in
    centos | sles) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/local/lib ;;
    *) return ;;
  esac
  pushd ${GREENPLUM_INSTALL_DIR}
    cp -d ${libdir}/libquicklz.so* lib
  popd
}

function include_libuv() {
  local includedir=/usr/include
  local libdir
  case "${TARGET_OS}" in
    centos | sles) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/lib/x86_64-linux-gnu ;;
    *) return ;;
  esac
  pushd ${GREENPLUM_INSTALL_DIR}
    # need to include both uv.h and uv/*.h
    cp -a ${includedir}/uv* include
    cp -a ${libdir}/libuv.so* lib
  popd
}

function export_gpdb() {
  TARBALL="${GPDB_ARTIFACTS_DIR}/${GPDB_BIN_FILENAME}"
  local server_version
  server_version="$("${GPDB_SRC_PATH}"/getversion --short)"

  local server_build
  server_build="${GPDB_ARTIFACTS_DIR}/server-build-${server_version}-${BLD_ARCH}${RC_BUILD_TYPE_GCS}.tar.gz"

  pushd ${GREENPLUM_INSTALL_DIR}
    source greenplum_path.sh
    python3 -m compileall -q -x test .
    chmod -R 755 .
    tar -czf "${TARBALL}" ./*
  popd

  cp "${TARBALL}" "${server_build}"
}

function export_gpdb_extensions() {
  pushd ${GPDB_SRC_PATH}/gpAux
    if ls greenplum-*zip* 1>/dev/null 2>&1; then
      chmod 755 greenplum-*zip*
      cp greenplum-*zip* "${GPDB_ARTIFACTS_DIR}/"
    fi
  popd
}

function export_gpdb_win32_ccl() {
    pushd ${GPDB_SRC_PATH}/gpAux
    if [ -f "$(find . -maxdepth 1 -name 'greenplum-*.msi' -print -quit)" ] ; then
        cp greenplum-*.msi "${GPDB_ARTIFACTS_DIR}/"
    fi
    popd
}

function export_gpdb_clients() {
  TARBALL="${GPDB_ARTIFACTS_DIR}/${GPDB_CL_FILENAME}"
  pushd ${GREENPLUM_CL_INSTALL_DIR}
    source ./greenplum_clients_path.sh
    mkdir -p bin/ext/gppylib
    cp ${GREENPLUM_INSTALL_DIR}/lib/python/gppylib/__init__.py ./bin/ext/gppylib
    cp  ${GREENPLUM_INSTALL_DIR}/lib/python/gppylib/gpversion.py ./bin/ext/gppylib
    # GPHOME_LOADERS and greenplum_loaders_path.sh are still requried by some users
    # So link greenplum_loaders_path.sh to greenplum_clients_path.sh for compatible
    ln -sf greenplum_clients_path.sh greenplum_loaders_path.sh
    chmod -R 755 .
    tar -czf "${TARBALL}" ./*
  popd
}

function build_xerces()
{
  OUTPUT_DIR="${GPDB_SRC_PATH}/gpAux/ext/${BLD_ARCH}"

  local libdir
  case "${TARGET_OS}" in
    centos | sles) libdir=/usr/lib64 ;;
    ubuntu) libdir=/usr/lib/x86_64-linux-gnu ;;
    *) return ;;
  esac

  local locallibdir
  case "${TARGET_OS}" in
    centos | sles) locallibdir=/usr/local/lib ;;
    # FIXME is ubuntu path correct?
    ubuntu) locallibdir=/usr/local/lib/x86_64-linux-gnu ;;
    *) return ;;
  esac

  mkdir -p "${OUTPUT_DIR}/lib"
  cp -a ${locallibdir}/libxerces-c*.so "${OUTPUT_DIR}/lib" || \
    cp -a ${libdir}/libxerces-c*.so "${OUTPUT_DIR}/lib"
}

function _main() {
  mkdir -p ${GPDB_SRC_PATH}/gpAux/ext

  case "${TARGET_OS}" in
    centos|ubuntu|sles)
      prep_env
      build_xerces
      install_deps
      ;;
    win32)
        export BLD_ARCH=win32
        ;;
    *)
        echo "only centos, ubuntu, sles and win32 are supported TARGET_OS'es"
        false
        ;;
  esac

  generate_build_number

  # By default, only GPDB Server binary is build.
  # Use BLD_TARGETS flag with appropriate value string to generate client, loaders
  # connectors binaries
  if [ -n "${BLD_TARGETS}" ]; then
    BLD_TARGET_OPTION=("BLD_TARGETS=\"$BLD_TARGETS\"")
  else
    BLD_TARGET_OPTION=("")
  fi

  export CONFIGURE_FLAGS=${CONFIGURE_FLAGS}

  build_gpdb "${BLD_TARGET_OPTION[@]}"
  build_mxdb
  git_info

  if [[ "${TARGET_OS}" != "win32" ]] && [[ -z "${SKIP_UNITTESTS}" ]]; then
      # Don't unit test when cross compiling. Tests don't build because they
      # require `./configure --with-zlib`.
      unittest_check_gpdb
  fi
  include_zstd
  include_quicklz
  include_libuv
  export_gpdb
  export_gpdb_extensions
  export_gpdb_win32_ccl

  if echo "${BLD_TARGETS}" | grep -qwi "clients"
  then
      export_gpdb_clients
  fi
}

_main "$@"
