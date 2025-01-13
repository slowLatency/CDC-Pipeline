USE DATABASE DB;
USE SCHEMA DIM_SCHEMA;

INSERT OVERWRITE INTO CUSTOMER_UPSERD (
SELECT $1:created_at as created_at, 
       $1:customer_address as customer_address, 
       $1:customer_id as customer_id, 
       $1:first_name as first_name, 
       $1:last_name as last_name
FROM @CDC_EXSTAGE
)