from google.cloud import bigquery
import sys

if len(sys.argv) != 3:
    raise Exception("Exactly 2 arguments are required: <inputUri> <outputUri>")

start = sys.argv[1]
end = sys.argv[2]

client = bigquery.Client()

# set test db
dataset_ref = client.dataset('gx_dataset')
dataset = bigquery.Dataset(dataset_ref)

# Write query results to a new table
job_config = bigquery.QueryJobConfig()
table_ref = dataset.table('erc20_daily_temp')
job_config.destination = table_ref
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

query_job = client.query(
    """
    SELECT 
        from_address, 
        to_address, 
        token_address, 
        value 
    FROM 
        `bigquery-public-data.ethereum_blockchain.token_transfers` 
    WHERE 
        transaction_hash 
    IN 
        (SELECT 
            DISTINCT `hash` AS transaction_hash 
        FROM 
            `bigquery-public-data.ethereum_blockchain.transactions` 
        WHERE 
            receipt_contract_address IS NOT NULL 
        AND 
            receipt_contract_address 
        IN 
            (SELECT 
                DISTINCT address 
            FROM 
                `bigquery-public-data.ethereum_blockchain.contracts` 
            WHERE 
                is_erc20 IS TRUE 
            ) 
        ) 
    AND 
        block_timestamp >= TIMESTAMP('{timestamp_start}') 
    AND 
        block_timestamp < TIMESTAMP('{timestamp_end}');
    """.format(timestamp_start=start, timestamp_end=end),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()