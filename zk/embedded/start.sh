#!/bin/bash

pidc=`jps -v | grep embedded-zookeeper | wc -l`
if [[ $pidc -ne 0 ]] ; then
    echo "test zookeeper already running"
else
    echo "starting test zookeeper"
    mkdir -p zookeeper-data
    java -Dname=embedded-zookeeper -jar zookeeper-3.4.9-fatjar.jar server zk.cfg > zookeeper-data/zookeeper.log &
fi
