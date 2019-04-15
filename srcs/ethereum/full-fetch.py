from google.cloud import bigquery
import sys

# CLI input
if len(sys.argv) != 2:
    raise Exception("Exactly 1 arguments are required: <today>")

today = sys.argv[1]

# create client
client = bigquery.Client()

# set db
dataset_ref = client.dataset('gx_dataset')
dataset = bigquery.Dataset(dataset_ref)

# Write query results to temporary table
job_config = bigquery.QueryJobConfig()
job_config.destination = dataset.table("eth_full_temp")
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
        block_timestamp < TIMESTAMP('{today}') 
    AND 
        value IS NOT NULL 
    UNION ALL 
    SELECT 
        to_address AS address, 
        value AS balance 
    FROM 
        `bigquery-public-data.ethereum_blockchain.transactions` 
    WHERE 
        to_address IS NOT NULL 
    AND 
        value IS NOT NULL 
    AND 
        block_timestamp < TIMESTAMP('{today}')
    """.format(today=today),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()