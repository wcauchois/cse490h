#!/bin/bash
protoc --java_out=proj/ proj/RPCProtos.proto
protoc --java_out=proj/ proj/transactions/Messages.proto
protoc --java_out=proj/ proj/paxos/Messages.proto
