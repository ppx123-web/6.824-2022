#!/usr/bin/env zsh

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1

for i in $(seq 1 $runs); do
    echo '***' TEST $i STARTS
    rm log.txt
    go test -run 2A &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    else
        echo '***' TEST $i PASSED
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
