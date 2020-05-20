#!/bin/bash
# eg: sh multy_test.sh out 100 2B
# eg: sh multy_test.sh out 100 (test all tests)
set -e
if [ $# -lt 2 ]; then
	echo "Usage: $0 [output file] [repeat time] [test name]"
	exit 1
fi

if [ $# -eq 3 ]; then
  for i in `seq $2`
  do
    echo "current round: $i"
    echo "count: $i" > $1
	  go test -run $3 >> $1
  done
fi

if [ $# -eq 2 ]; then
  for i in `seq $2`
  do
    echo "current round: $i"
    echo "count: $i" > $1
	  go test >> $1
  done
fi

echo "no failure occur!"