CREATE STREAM IF NOT EXISTS valid_users_stream AS
SELECT *
FROM cleaned_users_stream
WHERE data_quality_status = 'VALID'
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS gold_daily_country_stats AS
SELECT 
    country_code,
    
    COUNT(*) AS total_users,
    COUNT_DISTINCT(user_key) AS unique_users,
    CAST(ROUND(AVG(age_validated), 1) AS DOUBLE) AS avg_age,
    MIN(age_validated) AS min_age,
    MAX(age_validated) AS max_age,
    
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd') AS event_date,
    MAX(ROWTIME) AS last_updated

FROM valid_users_stream
WINDOW TUMBLING (SIZE 1 DAY)
GROUP BY 
    country_code
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS gold_hourly_stats AS
SELECT 
    country_code,
    
    COUNT(*) AS users_this_hour,
    COUNT_DISTINCT(user_key) AS unique_users_this_hour,
    CAST(ROUND(AVG(age_validated), 1) AS DOUBLE) AS avg_age_this_hour,
    
    TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:00') AS hour_bucket,
    MAX(ROWTIME) AS last_updated

FROM valid_users_stream
WINDOW TUMBLING (SIZE 1 HOUR)
GROUP BY 
    country_code
EMIT CHANGES;
