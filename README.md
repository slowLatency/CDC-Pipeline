# 🚀 CDC-Pipeline

## 🛠️ Setup an Airflow Project on Your Local

1. Install Astro CLI using `brew install astro`.  
2. Initialize an Airflow project using `astro dev init`.  
3. Start your local Airflow environment using `astro dev start`.  

---

## ⚙️ Components Involved

1. **Source Database:** Postgres 🐘  
2. **Target Destination:** Snowflake Warehouse ❄️  
3. **Schedule Interval (CDC Frequency):** Daily (Batch) ⏰  
4. **Source Tables:** `Customer`, `Customer_Metadata` 📋  
5. **Target Tables:** `Customer_Dim`, `Customer_Upserd` 📊  
6. **Load Type:**  
   - Full Load (for the initial load) 🟢  
   - Incremental Load (after the first load) 🔄  
7. **SCD Type:** SCD Type 2 📈  

---

## 📜 DAG Overview

### `cdc_pipeline.py`

1. **Load Type:** Determines whether the load will be a full load or incremental load.  
2. **Full Load or Incremental Load:** Executes the appropriate task based on the load type.  
3. **Postgres to Parquet:** Loads the `Customer` table from Postgres into a Parquet file.  
4. **Metadata Load:** Loads the `Customer_Metadata` table, which contains the `last_run_date`. This date will be used for the next run as part of a timestamp-based CDC implementation.  
5. **Parquet to S3:** Transfers the generated Parquet file to an S3 bucket, which serves as an external stage.  
6. **S3 to Snowflake:**  
   - Loads the Parquet file into the Snowflake table `Customer_Upserd`.  
   - A materialized view is created on this table to pre-compute results.  
   - This materialized view is used to load the `Customer_Dim` table incrementally (SCD Type 2).  

![CDC Pipeline Diagram](https://github.com/user-attachments/assets/f8357c42-dd2b-4705-ac8c-150c57e6cea7)

---

## 🔗 Connections Used

1. **Postgres Connection (`postgres_conn`):** Establishes a connection with the Postgres database.  
2. **AWS Connection (`aws_conn`):** Establishes a connection with AWS S3.  
3. **Snowflake Connection (`snowflake_conn`):** Establishes a connection with Snowflake.  
4. **Storage Integration Object:** Sets up an external stage for Snowflake.  

---

## 🛠️ Technologies Used

- **Apache Airflow** 🌬️: For orchestrating the entire pipeline.  
- **Postgres** 🐘: As the source database.  
- **Snowflake** ❄️: As the data warehouse.  
- **AWS S3** ☁️: For staging Parquet files as an external stage.  
- **Parquet** 📦: As the file format for transferring data.  
- **Docker** 🐳: To containerize and run the Airflow environment.  
- **Python** 🐍: For writing the DAG logic.  

---

## ✨ Highlights

- **Load Types:** Fully supports both full and incremental loads.  
- **SCD Implementation:** Implements SCD Type 2 for `Customer_Dim`.  
- **Cloud Integration:** Uses S3 as a staging area for data transfer between Postgres and Snowflake.  
- **Materialized Views:** Optimizes query performance in Snowflake using materialized views.  
- **Containerized Setup:** Fully containerized using Docker for easy deployment and scalability.

##  CDC Pipeline Architecture

![image](https://github.com/user-attachments/assets/9276fc68-9f91-46d6-a5d7-8a78dd40606b)

