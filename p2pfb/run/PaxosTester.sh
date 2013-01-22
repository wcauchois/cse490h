#!/bin/bash

# XXX
#if [ $# -gt 0 ]; then
#    FAIL_LVL=$1
#else
#    FAIL_LVL=0
#fi

FAIL_LVL=0

# copy "start 1\nstart 2\n...start N" to the clipboard
# if you want this bomb-ass functionality, colin, you have to apt-get install xclip
NUM_NODES=`python -c "print open('paxos.conf').readlines()[-1].split(':')[0]"`
if [ $USER == "billy" ]; then
    seq 0 $NUM_NODES | sed "s/^/start /" | xclip -selection c
elif [ $USER == "cs" ]; then
    seq 0 $NUM_NODES | sed "s/^/start /" | xclip
fi


rm -rf storage/
java -cp bin/:jars/* edu.washington.cs.cse490h.lib.MessageLayer -s \
-l partialOrder.log -L totalOrder.log -f $FAIL_LVL -n paxos.FaultTolerantNode $* \
 | egrep -v "(TIMEOUT|PSH)"
