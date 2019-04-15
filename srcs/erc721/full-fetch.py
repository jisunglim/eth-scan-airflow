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
job_config.destination = dataset.table("erc721_full_temp")
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
        block_timestamp < "{today}"
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
    """.format(today=today),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()