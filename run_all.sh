#!/bin/bash
./clean.sh
sleep 1
./btcd &
sleep 1
./btcd --mode=shard &
sleep 1
./btcd --mode=oracle &
sleep 2
ps ax | grep "./btcd" | cut -f1 -d" " | xargs kill -KILL
