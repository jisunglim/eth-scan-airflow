from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 15),
    'email': ['iejisung@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'erc20_daily_batch',
    default_args=default_args,
    schedule_interval="30 4 * * *")

t1 = BashOperator(
    task_id="erc20_bq_daily_fetch",
    bash_command="""
        python $AIRFLOW_HOME/srcs/erc20/daily-fetch.py {{yesterday_ds}} {{ds}}
    """,
    dag=dag)

t2 = BashOperator(
    task_id='erc20_bq_daily_batch',
    bash_command="""
        gcloud dataproc jobs submit pyspark $AIRFLOW_HOME/srcs/erc20/daily-batch.py \
            --cluster gx-cluster \
            --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar
    """,
    dag=dag)

t1 >> t2