#!/bin/bash

echo "killing test zookeeper"
jps -v | grep embedded-zookeeper | awk '{print $1}' | xargs kill -9
rm -rf zookeeper-data
