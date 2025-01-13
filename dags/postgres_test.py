# import os
# from airflow.decorators import dag, task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from datetime import datetime
# import pandas as pd



# @dag(
#     start_date = datetime(2025,1,1),
#     schedule_interval = '@daily',
#     template_searchpath = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql')
# )
# def cdc_dag():
#     """cdc pipeline dag"""


#     @task
#     def postgres_to_parquet():
#         """Extract customer data from postgres and load into parquet"""

#         postgres_hook = PostgresHook(postgres_conn_id = "postgres_conn")
#         conn = postgres_hook.get_conn()
#         cursor = conn.cursor()

#         query = "SELECT * FROM CUSTOMER"
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         columns = [desc[0] for desc in cursor.description]

#         df = pd.DataFrame(rows, columns = columns)
#         file_path = os.path.join(os.getcwd(), 'include', 'parquet', 'customer_p.parquet')
#         df.to_parquet(file_path, index=False)
        
#         return file_path 
    
#     @task
#     def parquet_to_s3(file_path):

#         s3_hook = S3Hook(aws_conn_id = 'aws_conn')
#         s3_key = 'landing/FullLoad/Customer/customer_p.parquet'
#         s3_hook.load_file(
#             filename = file_path,
#             key = s3_key,
#             bucket_name = "cdc-pipeline-staging",
#             replace = True
#         )

#         return s3_key

#     @task
#     def s3_to_snowflake():
#         """loading parquet from s3 to snowflake table"""

#         files = ['create_external_stage.sql', 'exstage_to_table.sql']

#         for sql_file in files:

#             file_path = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql', sql_file)

#             with open(file_path, 'r') as file:
#                 query = file.read()

#             print("Running ", sql_file)
#             sql_query = SQLExecuteQueryOperator(
#                 task_id = f'{sql_file}_execution',
#                 conn_id = 'snowflake_conn',
#                 sql = query
#             )
#             sql_query.execute({})

#         return True

    
#     parquet_file_path = postgres_to_parquet()

#     parquet_file_path >> parquet_to_s3(parquet_file_path) >> s3_to_snowflake()

# dag = cdc_dag()