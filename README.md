# How to run single-node cluster

Make sure you have _Oracle JDK 7_ installed. 
	
	export IMPALA_HOME=/home/user/path-to-impala
	export JAVA_HOME=path-to-jdk-dir

	. ./bin/impala-config.sh
	./testdata/bin/run-all.sh # start CDH environment
	./bin/start-impala=cluster.py -s 1 # start impalad,catalogd,statestore