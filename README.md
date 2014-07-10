# How to build

Make sure you have _Oracle JDK 7_ installed. 
    
    export IMPALA_HOME=/home/user/path-to-impala
    export JAVA_HOME=path-to-jdk-dir
    ./buildall.sh


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