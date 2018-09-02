#!/bin/bash
./clean.sh
sleep 1
./btcd --bootstrap --conf=config1.json --n=2 &
sleep 1
./btcd --mode=shard --conf=config_s10.json --n=2 &
sleep 1
./btcd --mode=shard --conf=config_s11.json --n=2 &
sleep 1
./btcd --mode=oracle --n=2 &
sleep 5
./btcd --conf=config2.json --n=2 &
sleep 1
./btcd --mode=shard --conf=config_s20.json --n=2 &
sleep 1
./btcd --mode=shard --conf=config_s21.json --n=2 &
sleep 5

ps ax | grep -w "./btcd" | cut -f1 -d" " | xargs kill -KILL
