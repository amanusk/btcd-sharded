#!/bin/bash
/bin/rm testlog.log


## Delete all tables from databse 
cockroach sql --format=csv -e 'SHOW TABLES FROM blockchain'  --insecure | tail -n +2   | xargs -n1 printf 'DROP TABLE blockchain."%s";\n'   | cockroach sql --insecure
