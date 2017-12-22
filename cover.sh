#!/usr/bin/env bash

set -e
echo "" > coverage.txt

for d in $(go list $@); do
    go test -race -coverprofile=profile.log $d
    if [ -f profile.log ]; then
        cat profile.log >> coverage.txt
        rm profile.log
    fi
done
