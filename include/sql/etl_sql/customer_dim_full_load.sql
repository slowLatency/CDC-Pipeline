USE DATABASE DB;
USE SCHEMA DIM_SCHEMA;

COPY INTO CUSTOMER_DIM
FROM (
    SELECT $1:customer_id::TEXT AS DIM_CUSTOMER_ID, 
    $1:first_name::TEXT AS DIM_FIRST_NAME, 
    $1:last_name::TEXT AS DIM_LAST_NAME, 
    $1:customer_address::TEXT AS DIM_ADDRESS, 
    $1:created_at::TEXT AS DIM_START_DATE, 
    '9999-12-31' AS DIM_END_DATE, 
    TRUE AS DIM_IS_ACTIVE
    FROM @CDC_EXSTAGE
    )
