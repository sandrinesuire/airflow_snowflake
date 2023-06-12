import os

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import datetime, timedelta
import pandas_datareader.data as web
from datetime import datetime

import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

yf.pdr_override()



DAG_ID = "csv_to_s3_5"
TEMP_FILE = "meta.csv"

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=60)
}


@task()
def get_and_save_data():
    data = web.get_data_yahoo('META', start='2007-01-01')

    date = str(datetime.today().date()).replace("-", "")
    file_name = f"{date}{TEMP_FILE}"
    data.to_csv(file_name)
    return file_name


@dag(
        dag_id=DAG_ID,
        default_args=default_args,
        description='This is our first dag that we write',
        start_date=datetime(2023, 5, 3),
        schedule_interval='@daily',
)
def first_dag():
    data_file_name = get_and_save_data()


    LocalFilesystemToS3Operator(
        task_id="create_local_to_s3_job",
        filename=data_file_name,
        dest_key=f"snowflake/{data_file_name}",
        dest_bucket=os.getenv('BUCKET'),
        replace=True,
        aws_conn_id="aws_conn",
    )


first_dag()



