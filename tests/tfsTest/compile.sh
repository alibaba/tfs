#!/bin/sh

cd ../gaia
sh exec.sh "install -Dmaven.test.skip=true"

mvn clean

mvn compile
