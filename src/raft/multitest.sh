if [ $# -ne 3 ]; then
    echo "Usage: $0 numTrials test threads"
    exit 1
fi
rm -r tmp/*
threads=0
if [ $1 -lt $3 ]; then
    threads=$1
else
    threads=20
fi
echo Test $2 $1 trials with $threads threads
./dtest -o ./tmp -n $1 $2 -p $3