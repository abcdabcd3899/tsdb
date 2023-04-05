MatrixDB
========

TODO

Packaging
---------

To build the package, please prepare a __clean__ clone of the repo, checkout
the target branch/tag, and prepare a dir for the results:

    # prepare the repo
    cd /path/to/matrixdb.git
    git submodule update --init --recursive

    # prepare the result dir
    mkdir -p /tmp/build/artifacts

Then launch the build job with docker:

    # build
    docker run \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash /tmp/src/matrixdb/PKG.CentOS.bash

The results are put under `/tmp/build/artifacts/`.

It is recommended to cleanup the results dir, `/tmp/build/artifacts/`, before
issuing a new build.

Enterprise or community build
-----------------------------

By default we do a community build, but we could do an enterprise build during
the packaging by specifying the option `--enable-enterprise`:

    # build with 16KB as the block size
    docker run \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash /tmp/src/matrixdb/PKG.CentOS.bash --enable-enterprise

Packaging with non-default block size
-------------------------------------

By default we build with 8KB as the block size, however it is possible to
specify it during the packaging, simply pass it to the `PKG.CentOS.bash`
script:

    # build with 16KB as the block size
    docker run \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash /tmp/src/matrixdb/PKG.CentOS.bash --with-blocksize=16

Testing
-------

Tests can be ran like below (you must first have the packages built like the
above):

    # suppose the build resutls are under /tmp/build/artifacts
    docker run \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build/artifacts:/tmp/build/artifacts:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash /tmp/src/matrixdb/TEST.CentOS.bash

If you want to collect all the test outputs, change the command as below, then
you can find all the sql outputs under `/tmp/build/`.

    # suppose the build resutls are under /tmp/build/artifacts
    docker run \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build:/tmp/build:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash /tmp/src/matrixdb/TEST.CentOS.bash

If you want to debug the tests, launch a docker container shell:

    # suppose the build resutls are under /tmp/build/artifacts
    docker run \
      --privileged \
      -w /tmp \
      -v $PWD:/tmp/src/matrixdb:ro \
      -v /tmp/build:/tmp/build:rw \
      -it pivotaldata/gpdb7-centos7-build \
      bash

Inside the container shell, you can manually run the tests by executing
`/tmp/src/matrixdb/TEST.CentOS.bash`, the container is alive even after the
tests, unless you exit from that shell.  So you get a chance to rerun some of
the tests or debug with gdb.

Verifying checksums
-------------------

We provide MD5 and SHA256 checksums of the rpm package, to verify with them,
download the rpm package and the checksum files, put them in the same dir, then
execute below commands:

    md5sum --check MD5SUMS
    sha256sum --check SHA256SUMS

We can also generate the checksums and compare them manually:

    cat MD5SUMS
    md5sum matrixdb-*.rpm

    cat SHA256SUMS
    sha256sum matrixdb-*.rpm
