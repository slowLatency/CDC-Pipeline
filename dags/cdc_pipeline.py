import os
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable
from dotenv import load_dotenv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


@dag(
    start_date = datetime(2025,1,1),
    schedule_interval = '@daily',
    template_searchpath = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql')
)
def CDC():
    """DAG for CDC Pipeline"""

    # load_dotenv('C:/Users/Lenovo/Desktop/Adib/CDCPipeline/.env')


    @task.branch
    def load_type():
        load_type = "inc"
        return load_type

    @task(task_id = 'full_load')
    def full_load():
        file_path = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql', 'customer.sql')
        with open(file_path, 'r') as f:
            query = f.read()
        return query.format(where_cond = 'where 1=1')
    
    @task(task_id = 'inc')
    def inc():
        file_path = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql', 'customer.sql')
        with open(file_path, 'r') as f:
            query = f.read()
        print(query.format(where_cond = "where created_at > (SELECT LAST_CREATED_DATE FROM CUSTOMER_METADATA)"))
        return query.format(where_cond = "where created_at > (SELECT LAST_CREATED_DATE FROM CUSTOMER_METADATA)")
 
    @task(trigger_rule = 'none_failed_or_skipped')
    def postgres_to_parquet(query):
        query = query.replace('"',"")
        print(query)
        # postgres_conn = os.getenv("postgres_conn")
        postgres_hook = PostgresHook(postgres_conn_id = "postgres_conn") #try to read it from .env file
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(query)
        rows = cursor.fetchall()

        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        file_path = os.path.join(os.getcwd(), 'include', 'parquet', 'customer.parquet')
        df.to_parquet(file_path, index=False)

        return file_path

    # @task
    # def metadata_load():

        
    #     # metadata_load.execute({})
    
    metadata_load = SQLExecuteQueryOperator(
            task_id = "metadata_load",
            conn_id = "postgres_conn",
            sql = "etl_metadata.sql"
        )
    
    @task
    def parquet_to_s3(file_path):

        s3_hook = S3Hook(aws_conn_id = "aws_conn")
        s3_hook.load_file(
            filename = file_path,
            key = 'landing/FullLoad/Customer/customer.parquet',
            bucket_name = 'cdc-pipeline-staging',
            replace = True
        )

    @task
    def s3_to_snowflake():

        sql_files = ["create_external_stage.sql", "customer_upserd.sql", "customer_scd_inc.sql"]
        # snowflake_conn = os.getenv("snowflake_conn")

        for file in sql_files:
            
            file_path = os.path.join(os.getcwd(), 'include', 'sql', 'etl_sql', file)
            with open(file_path, 'r') as f:
                query = f.read()
            
            print("Executing query:", file)
            query_execute = SQLExecuteQueryOperator(
                task_id = f"{file}_execution",
                conn_id = "snowflake_conn",
                sql = query
            )
            print(f"{file} execution complete")

            query_execute.execute({})
        
        return True
    
    # mode = load_type()
    # query = mode >> full_load()
    # print("Main Call")
    # print(query)
    # file_path = query >> postgres_to_parquet(query) 
    # file_path >> parquet_to_s3(file_path) >> s3_to_snowflake()

    mode = load_type()
    query = mode >> [full_load(), inc()]
    file_path = query >> postgres_to_parquet(query[1])
    file_path >> metadata_load >> parquet_to_s3(file_path) >> s3_to_snowflake()


dag = CDC()