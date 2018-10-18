#!/bin/bash
cd /home/ubuntu/go/src/github.com/btcsuite/btcd &&
git reset --hard &&
git pull origin sharding &&
go build

