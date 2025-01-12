# CDC-Pipeline:

Setup an Airflow project on your local:
  1.Install Astro CLI using brew install astro
  2.Initialize an Airflow project using astro dev init
  3.Start your local Airflow environment using astro dev start

# Components Involved:
  1.Source Database: Postgres
  2.Target Destination: Snowflake Warehouse
  3.Schedule Interval(CDC Frequency): Daily(Batch)
  4.Source Table: Customer, Customer_Metadata
  5.Target Table: Customer_Dim, Customer_Upserd
  6.Load Type: Full Load (for initial load), Incremental load (After the first Load)
  7.SCD Type: Scd Type 2

# DAG:
# Cdc_pipeline.py:
  1.Load_type: To determine whether the load type will be full load, incremental
  2.Full_load or inc: Depending on the load type either full load task or inc will be executed.
  3.Postgres_to_paruet: To load the customer table from postgres into a parquet file.
  4.Metadata_load: To load the customer_metadata table that will have the last_run_date, this last_run_date will be used in the next run as an implementation for Timestamp based CDC.
  5.Parquet_to_s3: Will load the generated parquet file into S3 bucket, which will in turn act as the external stage.
  6.S3_to_snowflake: To load the parquet file into the snowflake table Customer_Upserd, since this is an external table a materialized view was created on top of this table in-order to pre-compute the results. This table was then used in order to load the Customer_dim (SCD Type 2) incrementally

![image](https://github.com/user-attachments/assets/f8357c42-dd2b-4705-ac8c-150c57e6cea7)

# Connections Used:
1.Postgres_conn: To establish a connection with postgres
2.Aws_conn: To establish a connection with AWS S3
3.Snowflake_conn: To establish a connection with snowflake.
4.Storage integration object: To setup External Stage.
