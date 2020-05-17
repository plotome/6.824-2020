#!/bin/bash
# eg: sh multy_test.sh TestBackup2B out 100
set -e
if [ $# -ne 3 ]; then
	echo "Usage: $0 [test] [output file] [repeat time]"
	exit 1
fi

# export "GOPATH=$(git rev-parse --show-toplevel)"
# cd "${GOPATH}/src/raft"
for i in `seq $3`
do
  echo "current round: $i"
  echo "count: $i" > $2
	go test -run $1 >> $2
done