#!/bin/sh

LD_LIBRARY_PATH=/usr/local/staf/lib:${LD_LIBRARY_PATH}
MAVEN_HOME=/home/admin/workspace/mingyan.zc/apache-maven-3.0.3
JAVA_HOME=/usr/java/jdk1.6.0_20
PATH=$MAVEN_HOME/bin:$JAVA_HOME:${PATH}
CLASSPATH=/usr/local/staf/lib/JSTAF.jar:${CLASSPATH}
export LD_LIBRARY_PATH MAVEN_HOME PATH JAVA_HOME CLASSPATH
#env
mvn $1
