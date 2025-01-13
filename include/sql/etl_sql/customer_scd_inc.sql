USE DATABASE DB;
USE SCHEMA DIM_SCHEMA;

INSERT OVERWRITE INTO CUSTOMER_DIM
with this_year_data as (
SELECT *
FROM customer_upserd
),
last_year_data as (
SELECT *
FROM customer_dim
WHERE DIM_END_DATE = '9999-12-31'
),
historical_scd as (
select *
from customer_dim
where dim_end_date <> '9999-12-31'
),
unchanged_records as (
select ls.dim_customer_id,
ls.dim_first_name,
ls.dim_last_name,
ls.dim_address,
ls.dim_start_date,
'9999-12-31' as dim_end_date,
true as dim_is_active
from last_year_data ls 
LEFT join this_year_data ts 
on ls.DIM_customer_id = ts.customer_id
where ls.DIM_address = ts.CUSTOMER_address
OR ts.customer_id is NULL
),
changed_grp_records as (
select ls.dim_customer_id,
ls.dim_first_name,
ls.dim_last_name,
ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('address', ls.dim_address, 'start_date', ls.dim_start_date, 'end_date', ts.CREATED_AT, 'active_flag', false),
OBJECT_CONSTRUCT('address', ts.customer_address, 'start_date', ts.CREATED_AT, 'end_date', '9999-12-31', 'active_flag', true)) as arr_col
from last_year_data ls
inner join this_year_data ts
on ls.dim_customer_id = ts.customer_id
where ls.dim_address <> ts.customer_address
),
changed_records as (
select dim_customer_id,
dim_first_name,
dim_last_name,
t.value:address as dim_address,
t.value:start_date as start_date,
t.value:end_date as end_date,
t.value:active_flag as dim_is_active
from changed_grp_records, TABLE(FLATTEN(INPUT => arr_col)) AS t
),
new_records as (
select ts.customer_id,
ts.first_name,
ts.last_name,
ts.customer_address,
ts.CREATED_AT as dim_start_date,
'9999-12-31' as dim_end_date,
true as dim_is_active
from this_year_data ts
left join last_year_data ls
on ts.customer_id = ls.dim_customer_id
where ls.dim_customer_id is NULL
)

SELECT * FROM HISTORICAL_SCD
UNION
SELECT * FROM CHANGED_RECORDS
UNION
SELECT * FROM UNCHANGED_RECORDS
UNION
SELECT * FROM NEW_RECORDS
ORDER BY DIM_FIRST_NAME