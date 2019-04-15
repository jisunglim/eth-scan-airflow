from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 14),
    'email': ['iejisung@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'erc721_full_batch',
    default_args=default_args,
    schedule_interval='@once')

t1 = BashOperator(
    task_id="erc721_bq_full_fetch",
    bash_command="""
        python $AIRFLOW_HOME/srcs/erc721/full-fetch.py {{ds}}
    """,
    dag=dag)

t2 = BashOperator(
    task_id='erc721_bq_full_batch',
    bash_command="""
        gcloud dataproc jobs submit pyspark $AIRFLOW_HOME/srcs/erc721/full-batch.py \
            --cluster gx-cluster \
            --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar
    """,
    dag=dag)

t1 >> t2