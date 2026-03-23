-- -- DROP STREAM IF EXISTS raw_users_stream;
-- CREATE STREAM IF NOT EXISTS raw_users_stream (
--     user_id VARCHAR,
--     username VARCHAR,
--     uuid VARCHAR,
--     email VARCHAR,
--     name VARCHAR,
--     first_name VARCHAR,
--     last_name VARCHAR,
--     gender VARCHAR,
--     age INTEGER,
--     country VARCHAR,
--     city VARCHAR,
--     phone VARCHAR,
--     job VARCHAR,
--     company VARCHAR,
--     ipv4 VARCHAR,
--     ipv6 VARCHAR,
--     fetched_at VARCHAR,
--     source VARCHAR,
--     api_version VARCHAR
-- ) WITH (
--     KAFKA_TOPIC = 'Raw_Data',
--     VALUE_FORMAT = 'JSON',
--     PARTITIONS = 3
-- );

-- SHOW STREAMS;
SELECT * FROM cleaned_users_stream;
SELECT * FROM valid_users_table;
SELECT * FROM invalid_users_dlq;
