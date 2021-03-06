#%%
from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext

# Create spark context
sc = pyspark.SparkContext()

# get Hadoop configurations
project_id = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket_id = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

# set input directory (if exist, delete first)
input_directory_full = 'gs://{}/hadoop/erc20/daily/full/pyspark_input'.format(bucket_id)
input_directory_daily = 'gs://{}/hadoop/erc20/daily/daily/pyspark_input'.format(bucket_id)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_full)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_daily)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)


# input bq dataset info
bq_input_project_id = project_id
bq_input_dataset_id = 'gx_dataset'
bq_input_table_id_full = 'erc20'
bq_input_table_id_daily = 'erc20_daily_temp'

# output bq table info
bq_output_project_id = project_id
bq_output_dataset_id = 'gx_dataset'
bq_output_table_id = 'erc20'

# RDD config input
full_conf = {
    'mapred.bq.project.id': project_id,
    'mapred.bq.gcs.bucket': bucket_id,
    'mapred.bq.temp.gcs.path': input_directory_full,
    'mapred.bq.input.project.id': bq_input_project_id,
    'mapred.bq.input.dataset.id': bq_input_dataset_id,
    'mapred.bq.input.table.id': bq_input_table_id_full,
}

daily_conf = {
    'mapred.bq.project.id': project_id,
    'mapred.bq.gcs.bucket': bucket_id,
    'mapred.bq.temp.gcs.path': input_directory_daily,
    'mapred.bq.input.project.id': bq_input_project_id,
    'mapred.bq.input.dataset.id': bq_input_dataset_id,
    'mapred.bq.input.table.id': bq_input_table_id_daily,
}

# Load RDD from BigQuery via GCS
rdd_full = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=full_conf)

rdd_daily = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=daily_conf)


def split(x):
    tasks = []
    if 'to_address' in x.keys():
        tasks.append(((x['to_address'], x['token_address']), int(x['value'])))
    if 'fromaddress' in x.keys():
        tasks.append(
            ((x['from_address'], x['token_address']), -int(x['value'])))
    return tasks

# Process
rdd_result = rdd_daily.map(lambda record: json.loads(record[1])) \
        .flatMap(split) \
        .union(
            rdd_full.map(lambda record: json.loads(record[1])).map(lambda x: ((x['address'],x['token_address']), int(x['balance'])))
        ) \
        .reduceByKey(lambda bal1, bal2: bal1+bal2) \
        .map(lambda x: (x[0][0], x[0][1], str(x[1])))

# result sample logging
pprint.pprint(rdd_result.take(5))

# Save to GCS temporarily
output_directory = 'gs://{}/hadoop/erc20/daily/pyspark_output'.format(bucket_id)
output_files = output_directory + '/part-*'

# sparSql context
sql_context = SQLContext(sc)

rdd_result \
    .toDF(['address', 'token_address', 'balance']) \
    .write.format('csv') \
    .save(output_directory)

# export to BigQuery
subprocess.check_call(
    'bq load --source_format CSV '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files} address:STRING,token_address:STRING,balance:STRING'
    .format(
        dataset=bq_output_dataset_id,
        table=bq_output_table_id,
        files=output_files).split())

# Clean up the temporary directories
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_full)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_daily)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)
