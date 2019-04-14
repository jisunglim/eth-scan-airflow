/Users/jaylim/anaconda3/envs/dash/bin/python ./fetch-full-batch.py

gcloud dataproc jobs submit pyspark ./full-batch.py \
        --cluster gx-cluster \
        --jars=gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar
