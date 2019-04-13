#%%
from google.cloud import bigquery

#%%
# bq_client
bq_client = bigquery.Client()

#%%
# set test db
dataset_ref = bq_client.dataset('gx_dataset')
dataset = bigquery.Dataset(dataset_ref)

#%%
# Write query results to a new table
job_config = bigquery.QueryJobConfig()
job_config.destination = dataset.table("transactions_full_temp")
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

query_job = bq_client.query(
    """
    SELECT 
        address, 
        SUM(value) as daily_balance_change 
    FROM 
        (SELECT 
            from_address AS address, 
            (-1) * (value) AS value 
        FROM 
            `bigquery-public-data.ethereum_blockchain.transactions` 
        WHERE 
            block_timestamp < '2019-04-01 00:00:00' 
        UNION ALL 
        SELECT 
            to_address AS address, 
            value AS value 
        FROM 
            `bigquery-public-data.ethereum_blockchain.transactions` 
        WHERE 
            block_timestamp < '2019-04-01 00:00:00')
    GROUP BY 
        address;
    """,
    location='US',  # Location must match dataset
    job_config=job_config)

query_job.result()

# rows = list(query_job)  # Waits for the query to finish

# Export table to GCS
# destination_uri = "gs://gx-bucket/daily/temp/transactions-2019-04-01.csv"
# dataset_ref = bq_client.dataset("gx_dataset", project="gx-project-190412")
# table_ref = dataset_ref.table("transactions_temp")

# extract_job = bq_client.extract_table(table_ref, destination_uri, location='US')
# extract_job.result()  # Waits for job to complete