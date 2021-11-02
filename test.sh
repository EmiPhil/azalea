#!/bin/bash
set -ex

logfile=/tmp/rlog

go test -v -race -run $@ |& tee ${logfile}

go run ./testlog-viz/main.go < ${logfile}

# usage:
# RAFT_FORCE_MORE_REELECTION=true ./test.sh TestElectionBasic