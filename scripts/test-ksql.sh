# docker compose exec -T ksqldb-cli ksql $KSQLDB_URL < /opt/ksql/create_streams.sql
# docker compose exec -T ksqldb-cli ksql $KSQLDB_URL < /opt/ksql/silver_transform.sql
# docker compose exec -T ksqldb-cli ksql $KSQLDB_URL < /opt/ksql/gold_transform.sql
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088 -f /opt/ksql/create_streams.sql
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088 -f /opt/ksql/transform_to_silver.sql
docker compose exec ksqldb-cli ksql http://ksqldb-server:8088 -f /opt/ksql/gold_transform.sql


docker compose exec -T ksqldb-cli ksql $KSQLDB_URL -e "SHOW STREAMS;"

docker compose exec -T ksqldb-cli ksql $KSQLDB_URL -e "SHOW TABLES;"