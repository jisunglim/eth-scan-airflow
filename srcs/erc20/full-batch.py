#%%
from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext

#%%
sc = pyspark.SparkContext()

# get Hadoop configurations from Google Cloud Dataproc
project_id = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket_id = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

# set input directory
input_directory = 'gs://{}/hadoop/erc20/full/pyspark_input'.format(bucket_id)

# input bq dataset info
bq_input_project_id = project_id
bq_input_dataset_id = 'gx_dataset'
bq_input_table_id = 'erc20_full_temp'

# output bq table info
bq_output_project_id = project_id
bq_output_dataset_id = 'gx_dataset'
bq_output_table_id = 'erc20'

# RDD config input
conf = {
    # Input Parameters.
    'mapred.bq.project.id': project_id,
    'mapred.bq.gcs.bucket': bucket_id,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': bq_input_project_id,
    'mapred.bq.input.dataset.id': bq_input_dataset_id,
    'mapred.bq.input.table.id': bq_input_table_id,
}

# Load data in from BigQuery.
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

pprint.pprint('Number of partitions: {}'.format(table_data.getNumPartitions()))

def split(x):
    tasks = []
    if 'to_address' in x.keys():
        tasks.append(((x['to_address'], x['token_address']), int(x['value'])))
    if 'fromaddress' in x.keys():
        tasks.append(((x['from_address'], x['token_address']), -int(x['value'])))
    return tasks

# Perform word count.
balances = table_data \
        .map(lambda record: json.loads(record[1])) \
        .flatMap(split) \
        .reduceByKey(lambda bal1, bal2: bal1+bal2) \
        .map(lambda x: (x[0][0], x[0][1], str(x[1])))
# .map(lambda x: (x[0], (1.0*x[1])/(10**18)))

# Display 10 results.
pprint.pprint(balances.take(5))

# # Stage data formatted as newline-delimited JSON in Cloud Storage.
output_directory = 'gs://{}/hadoop/erc20/full/pyspark_output'.format(
    bucket_id)
output_files = output_directory + '/part-*'

sql_context = SQLContext(sc)

balances \
    .toDF(['address', 'token_address', 'balance']) \
    .write.format('csv') \
    .save(output_directory)

# Shell out to bq CLI to perform BigQuery import.
subprocess.check_call(
    'bq load --source_format CSV '
    '--replace '
    '--autodetect '
    '{dataset}.{table} {files} address:STRING,token_address:STRING,balance:STRING'.format(
        dataset=bq_output_dataset_id,
        table=bq_output_table_id,
        files=output_files).split())

# Manually clean up the staging_directories, otherwise BigQuery
# files will remain indefinitely.

input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(
    output_path, True)
