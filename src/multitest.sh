#! /usr/bin/bash

if [ $# -lt 3 ]; then
    echo "Usage: $0 workdir numTrials threads test [race]"
    exit 1
fi
rm -r tmp/*

dir=$1
nums=$2
threads=$3
testname=$4
EXEC_PARAMS=${@:5}

cd $dir

echo Test $2 $1 trials with $threads threads
../dtest -v -v -o ./$dir/tmp -n $nums -p $threads $EXEC_PARAMS $testname