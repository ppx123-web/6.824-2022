#!/usr/bin/env bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 numTrials test"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1
tests=$2
dir=tmp
log=$dir/log.txt
test=$dir/test.txt

testcases=(Persist12C Persist22C Persist32C Figure82C UnreliableAgree2C Figure8Unreliable2C ReliableChurn2C UnreliableChurn2C)

rm $test

for i in $(seq 1 $runs); do
    echo '***' TEST $i STARTS
    echo '***' TEST $i STARTS >> $test
    rm $log
    time go test -run $2 >> $test &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    else
        if [ `grep -c "FAIL" $test` -ne '0' ]; then 
            echo '***' TEST $i FAILED 
            echo '***' TEST $i FAILED >> $test
            exit 1
        else
            echo '***' TEST $i OF $runs PASSED
            echo '***' TEST $i PASSED >> $test
        fi
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
