#! /usr/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $0 numTrials test threads [race][other params]"
    exit 1
fi
rm -r tmp/*
threads=0
if [ $1 -lt $3 ]; then
    threads=$1
else
    threads=$3
fi
nums=$1
testname=$2
EXEC_PARAMS=${@:4}
echo Test $2 $1 trials with $threads threads
./dtest -v -o ./tmp -n $nums -p $threads $EXEC_PARAMS $testname