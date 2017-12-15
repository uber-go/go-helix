#!/usr/bin/env bash

set -e
echo "" > cover.log

for d in $(go list $@); do
    go test -race -coverprofile=profile.log $d
    if [ -f profile.log ]; then
        cat profile.log >> cover.log
        rm profile.log
    fi
done
