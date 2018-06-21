## cockroachDB commands

add user:
cockroach user set amanusk --insecure

cockroach sql --insecure -e 'create database blockchain'

Grant privileges
cockroach sql --insecure -e 'GRANT ALL ON DATABASE blockchain TO amanusk'

## Delete all tables from databse 
cockroach sql --format=csv -e 'SHOW TABLES FROM blockchain'  --insecure | tail -n +3   | xargs -n1 printf 'DROP TABLE blockchain."%s";\n'   | cockroach sql --insecure
