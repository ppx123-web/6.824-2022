#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 numTrials test"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1
tests=$2
log=log.txt

for i in $(seq 1 $runs); do
    echo '***' TEST $i STARTS
    rm $log
    time go test -run $2 &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    else
        if [ `grep -c "FAIL" $log` -ne '0' ]; then 
            echo '***' TEST $i FAILED
            exit 1
        else
            echo '***' TEST $i PASSED
        fi
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
