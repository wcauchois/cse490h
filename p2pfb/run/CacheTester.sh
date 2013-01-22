#!/bin/bash
if [ "$1" = "-h" -o "$1" = "--help" ]; then
    ./scripts/CacheTesterScript.py --help
    exit 1
fi
rm -rf storage/
./scripts/CacheTesterScript.py $* | java -cp bin/:jars/* edu.washington.cs.cse490h.lib.MessageLayer -s -f 0 -n tests.CacheTester
