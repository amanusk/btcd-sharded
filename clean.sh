#!/bin/bash

./kill_all.sh

/bin/rm *.log

# /bin/rm -rf testdb/blocks_ffldb/
# /bin/rm -rf testdb/testdb*
# cp -r ~/.btcd/data/testnet/blocks_ffldb/ testdb/

## Delete all tables from databse
#cockroach sql --format=csv -e 'SHOW TABLES FROM blockchain'  --insecure | tail -n +2   | xargs -n1 printf 'DROP TABLE blockchain."%s";\n'   | cockroach sql --insecure

#cockroach sql --format=csv -e 'SHOW TABLES FROM blockchain'  --insecure --port=26258 | tail -n +2   | xargs -n1 printf 'DROP TABLE blockchain."%s";\n'   | cockroach sql --insecure --port=26258
