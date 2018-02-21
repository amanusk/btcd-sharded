#!/bin/bash
ps ax | grep "./btcd" | cut -f1 -d" " | xargs kill -KILL
pgrep "btcd" | xargs kill -KILL
