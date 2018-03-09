## cockroachDB commands

add user:
cockroach user set testuser --insecure

Grant privileges
cockroach sql --insecure -e 'GRANT ALL ON DATABASE school TO testuser'

## Delete all tables from databse 
cockroach sql --format=csv -e 'SHOW TABLES FROM blockchain'  --insecure | tail -n +3   | xargs -n1 printf 'DROP TABLE blockchain."%s";\n'   | cockroach sql --insecure
