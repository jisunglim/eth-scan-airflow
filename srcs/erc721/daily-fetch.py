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

# Write query results to temporary table
job_config = bigquery.QueryJobConfig()
table_ref = dataset.table('erc721_daily_temp')
job_config.destination = table_ref
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

query_job = client.query(
    """
    -- debit
    SELECT 
        from_address, 
        to_address, 
        token_address, 
        value
    FROM 
        `bigquery-public-data.ethereum_blockchain.token_transfers`
    WHERE 
        block_timestamp >= "{timestamp_start}"
    AND 
        block_timestamp < "{timestamp_end}"
    AND 
        from_address 
    IN 
        (SELECT 
            address 
        FROM 
            `bigquery-public-data.ethereum_blockchain.contracts`
        WHERE 
            is_erc721 IS TRUE)
    OR 
        to_address 
    IN 
        (SELECT 
            address 
        FROM 
            `bigquery-public-data.ethereum_blockchain.contracts`
        WHERE 
            is_erc721 IS TRUE)
    """.format(timestamp_start=start, timestamp_end=end),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()