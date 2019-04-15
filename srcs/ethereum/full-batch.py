#%%
from __future__ import absolute_import
import json
import pprint
import subprocess
import pyspark
from pyspark.sql import SQLContext

# Create spark context
sc = pyspark.SparkContext()
sc._jsc.hadoopConfiguration().setInt("mapred.max.split.size", 10000000)

# get Hadoop configurations
project_id = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
bucket_id = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')

# set input directory (if exist, delete first)
input_directory = 'gs://{}/hadoop/eth/full/pyspark_input'.format(bucket_id)
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

# input bq dataset info
bq_input_project_id = project_id
bq_input_dataset_id = 'gx_dataset'
bq_input_table_id = 'eth_full_temp'

# output bq table info
bq_output_project_id = project_id
bq_output_dataset_id = 'gx_dataset'
bq_output_table_id = 'eth'


# RDD config input
conf = {
    'mapred.bq.project.id': project_id,
    'mapred.bq.gcs.bucket': bucket_id,
    'mapred.bq.temp.gcs.path': input_directory,
    'mapred.bq.input.project.id': bq_input_project_id,
    'mapred.bq.input.dataset.id': bq_input_dataset_id,
    'mapred.bq.input.table.id': bq_input_table_id,
}


# Load RDD from BigQuery via GCS
rdd = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf=conf)

pprint.pprint('Number of partitions: {}'.format(rdd.getNumPartitions()))

# Process
rdd_result = rdd.map(lambda record: json.loads(record[1])) \
        .map(lambda x: (x['address'], int(x['value']))) \
        .reduceByKey(lambda bal1, bal2: bal1+bal2)

# result sample logging
pprint.pprint(rdd_result.take(5))

# Save to GCS temporarily
output_directory = 'gs://{}/hadoop/eth/full/pyspark_output'.format(bucket_id)
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
    '{dataset}.{table} {files} address:STRING,balance:NUMERIC'.format(
        dataset=bq_output_dataset_id,
        table=bq_output_table_id,
        files=output_files).split())

# Clean up the temporary directories
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)
output_path = sc._jvm.org.apache.hadoop.fs.Path(output_directory)
output_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(output_path, True)