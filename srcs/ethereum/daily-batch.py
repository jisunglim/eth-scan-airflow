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
input_directory_full = 'gs://{}/hadoop/eth/daily/full/pyspark_input'.format(bucket_id)
input_directory_daily = 'gs://{}/hadoop/eth/daily/daily/pyspark_input'.format(bucket_id)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_full)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_daily)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

# input bq dataset info
bq_input_project_id = project_id
bq_input_dataset_id = 'gx_dataset'
bq_input_table_id_full = 'eth'
bq_input_table_id_daily = 'eth_daily_temp'

# output bq table info
bq_output_project_id = project_id
bq_output_dataset_id = 'gx_dataset'
bq_output_table_id = 'eth'

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

def format_input(x):
    if ('balance' in x.keys()):
        return (x['address'], int(x['balance']))
    else:
        return (x['address'], 0)

# Process
rdd_result = rdd_daily.map(lambda record: json.loads(record[1])) \
        .map(lambda x: (x['address'], int(x['balance']))) \
        .union(
            rdd_full.map(lambda record: json.loads(record[1])) \
                .map(format_input)
        ) \
        .reduceByKey(lambda x, y: x+y) \
        .map(lambda x: (x[0], str(x[1])))

# result sample logging
pprint.pprint(rdd_result.take(5))

# Save to GCS temporarily
output_directory = 'gs://{}/hadoop/eth/daily/pyspark_output'.format(
    bucket_id)
output_files = output_directory + '/part-*'

# sparSql context
sql_context = SQLContext(sc)

rdd_result \
    .toDF(['address', 'balance']) \
    .write.format('csv') \
    .save(output_directory)

# export to BigQuery
subprocess.check_call(
    'bq load --source_format CSV '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files} address:STRING,balance:STRING'.format(
        dataset=bq_output_dataset_id,
        table=bq_output_table_id,
        files=output_files).split())

# Clean up the temporary directories
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_full)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory_daily)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(output_path, True)
