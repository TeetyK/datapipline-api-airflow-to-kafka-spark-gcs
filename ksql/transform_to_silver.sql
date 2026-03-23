CREATE STREAM IF NOT EXISTS cleaned_users_stream AS
SELECT 
    COALESCE(user_id, uuid) AS user_key,
    uuid,
    
    TRIM(name) AS name,
    LCASE(TRIM(email)) AS email_normalized,
    
    CASE 
        WHEN age BETWEEN 0 AND 120 THEN age
        ELSE NULL 
    END AS age_validated,
    CASE 
        WHEN UCASE(country) IN ('TH', 'THAILAND') THEN 'TH'
        WHEN UCASE(country) IN ('US', 'USA', 'UNITED STATES') THEN 'US'
        WHEN UCASE(country) IN ('GB', 'UK', 'UNITED KINGDOM') THEN 'GB'
        ELSE UCASE(country)
    END AS country_code,
    CAST(fetched_at AS TIMESTAMP) AS fetched_timestamp,
    CASE 
        WHEN email LIKE '%@%.%' 
        AND age BETWEEN 0 AND 120 
        AND country IS NOT NULL 
        AND LEN(TRIM(name)) > 0
        THEN 'VALID'
        ELSE 'INVALID'
    END AS data_quality_status,
    
    source,
    api_version,
    ROWTIME AS processed_at

FROM raw_users_stream
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS valid_users_table AS
SELECT 
    user_key,
    LATEST_BY_OFFSET(uuid) AS uuid,
    LATEST_BY_OFFSET(name) AS name,
    LATEST_BY_OFFSET(email_normalized) AS email,
    LATEST_BY_OFFSET(age_validated) AS age,
    LATEST_BY_OFFSET(country_code) AS country,
    LATEST_BY_OFFSET(fetched_timestamp) AS last_seen,
    LATEST_BY_OFFSET(processed_at) AS last_processed,
    COUNT(*) AS record_count

FROM cleaned_users_stream
WHERE data_quality_status = 'VALID'
GROUP BY user_key
EMIT CHANGES;
CREATE STREAM IF NOT EXISTS invalid_users_dlq AS
SELECT 
    *,
    'INVALID_RECORD' AS error_type,
    ROWTIME AS rejected_at

FROM cleaned_users_stream
WHERE data_quality_status = 'INVALID'
EMIT CHANGES;