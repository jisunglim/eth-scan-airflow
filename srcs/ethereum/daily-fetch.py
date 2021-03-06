from google.cloud import bigquery
import sys

# CLI input
if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <start_date> <end_date>")

start = sys.argv[1]
end = sys.argv[2]

# create client
client = bigquery.Client()

# set db
dataset_ref = client.dataset('gx_dataset')
dataset = bigquery.Dataset(dataset_ref)

# Append query results to the eth balance table
job_config = bigquery.QueryJobConfig()
table_ref = dataset.table("eth_daily_temp")
job_config.destination = table_ref
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

query_job = client.query(
    """
    SELECT 
        from_address AS address, 
        (-1) * (value) AS balance 
    FROM 
        `bigquery-public-data.ethereum_blockchain.transactions` 
    WHERE 
        block_timestamp >= TIMESTAMP('{timestamp_start}') 
    AND 
        block_timestamp < TIMESTAMP('{timestamp_end}') 
    AND 
        value IS NOT NULL 
    UNION ALL 
    SELECT 
        to_address AS address, 
        value AS balance 
    FROM 
        `bigquery-public-data.ethereum_blockchain.transactions` 
    WHERE 
        block_timestamp >= TIMESTAMP('{timestamp_start}') 
    AND 
        block_timestamp < TIMESTAMP('{timestamp_end}') 
    AND 
        value IS NOT NULL 
    AND 
        to_address IS NOT NULL
    """.format(timestamp_start=start, timestamp_end=end),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()