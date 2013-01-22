#!/bin/bash

rm -rf storage/
java -cp bin/:jars/plume.jar edu.washington.cs.cse490h.lib.MessageLayer -s -n RIOTester -f -0 -c scripts/SimpleRIOTest #-r 1294790397313
