# How to build

Make sure you have _Oracle JDK 7_ installed. 
    
    export IMPALA_HOME=/home/user/path-to-impala
    export JAVA_HOME=path-to-jdk-dir

## Boost

You will need _Boost 1.46.1_ installed. CMake will search system directories for it. If you have _Boost_ installed at another location do the following:

+ Add `set(BOOST_ROOT "<boost-install-dir")` to `CMakeLists.txt`
+ Add `set(Boost_NO_SYSTEM_PATHS ON)` to `CMakeLists.txt`
+ Add `"-I${Boost_INCLUDE_DIRS}"` to `CLANG_INCLUDE_FLAGS` of `be/CMakeLists.txt`
+ Add `"-I${Boost_INCLUDE_DIRS}"` to `IR_COMPILE_FLAGS` of `be/CMakeLists.txt`
+ Remove `COMPILE_TO_IR` function from `be/src/udf_samples/CMakeLists.txt`

## LLVM

    wget http://llvm.org/releases/3.3/llvm-3.3.src.tar.gz
    tar xvzf llvm-3.3.src.tar.gz
    cd llvm-3.3.src/tools
    svn co http://llvm.org/svn/llvm-project/cfe/tags/RELEASE_33/final/ clang
    cd ../projects
    svn co http://llvm.org/svn/llvm-project/compiler-rt/tags/RELEASE_33/final/ compiler-rt
    cd ..
    ./configure --with-pic
    make -j4 REQUIRES_RTTI=1
    sudo make install

You can skip installing and just set `LLVM_HOME` environment variable to build `bin` directory.

## Archlinux specific

+ Add `-lcrypto` to `be/CMakeLists.txt`
+ Add ``PYTHON=`which python2` `` to `be/build_thirdparty.sh` _THRIFT_ configure section
+ Add ``GFLAGS_INSTALL=`pwd`/third-party-install`` to `be/build_thirdparty.sh` _GLOG_ configure section
+ Replace `python` with `python2` at `bin/impala-shell.sh`
+ Replace `python` with `python2` at `shell/impala-shell`
+ Comment out extending `PYTHONPATH` with _THRIFT_ directories at `bin/set-pythonpath.sh`# 
+ Remove `DECLARE_int32(logbufsecs);` from `be/src/common/init.cc`

Workaroung somehow python2/3 issue:

    ln -s `which python2` python
    export PATH=`pwd`:$PATH

## Build script

    ./buildall.sh

If you have to rebuild you better use

    ./buildall.sh -noclean 
    # OR
    make impalad
    # OR
    make plan-fragment-executor-test

# How to run single-node cluster

	. ./bin/impala-config.sh
	./testdata/bin/run-all.sh # start CDH environment
	./bin/start-impala=cluster.py -s 1 # start impalad,catalogd,statestore

# How to stop cluster

	./bin/start-impala=cluster.py --kill # kills just impala,catalog,statestore
	./testdata/bin/kill-all.sh # kills CDH env and impala cluster

# Run _executor_ test

It depends on `TExecPlanFragmentParams.bin` file containing 'executor-ready' serialized query `select 1002;`.

    ./be/build/runtime/plan-fragment-executor-tests