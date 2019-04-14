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
table_ref = dataset.table("balances")
job_config.destination = table_ref
job_config.create_disposition = bigquery.CreateDisposition.CREATE_NEVER
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

query_job = client.query(
    """
    SELECT 
        address, 
        SUM(value) as balance 
    FROM 
        (SELECT 
            from_address AS address, 
            (-1) * (value) AS value 
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
            value AS value 
        FROM 
            `bigquery-public-data.ethereum_blockchain.transactions` 
        WHERE 
            block_timestamp >= TIMESTAMP('{timestamp_start}') 
        AND 
            block_timestamp < TIMESTAMP('{timestamp_end}') 
        AND 
            value IS NOT NULL 
        AND 
            to_address IS NOT NULL ) 
    GROUP BY 
        address;
    """.format(timestamp_start=start, timestamp_end=end),
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()