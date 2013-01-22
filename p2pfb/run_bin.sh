#!/bin/bash
# failure levels:
#  0 - nothing
#  1 - crash
#  2 - drop
#  3 - delay
#  4 - everything

FAIL=1
echo "FAILURE LEVEL = $FAIL"
rm -rf storage/
java -cp bin/:jars/* edu.washington.cs.cse490h.lib.MessageLayer -s -l partialOrder.log -L totalOrder.log -f $FAIL -n $*
