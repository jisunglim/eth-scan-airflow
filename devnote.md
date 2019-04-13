# Development Note

## Part 1: Ethereum Balance

### Full Batch (- 13 April, 2019)

1. `bigquery-public-data`에서 `13 April, 2019` 까지의 데이터 query하여 temp table에 저장하기

    - file: `./src/fetch-full-batch.py`

    - from: `bigquery-public-data.ethereum_blockchain.transactions`

    - to: `gx-project.gx_dataset.transactions_full_temp`

        ```python
        job_config = bigquery.QueryJobConfig()
        job_config.destination
            = dataset.table("transactions_full_temp")
        job_config.create_disposition
            = bigquery.CreateDisposition.CREATE_IF_NEEDED
        job_config.write_disposition
            = bigquery.WriteDisposition.WRITE_TRUNCATE
        ```

    - condition: `WHERE block_timestamp < '2019-04-01 00:00:00'`

2. spark 이용하여, full batch process 진행하기

    - file: `./src/full-batch.py`

    - from: `gx-project.gx_dataset.transactions_full_temp`

    - to: `gx-project.gx_dataset.balances`

    - spark code:

        ```python
        def split(x):
            if 'to_address' in x.keys():
                return [
                    (x['from_address'], -int(x['value'])),
                    (x['to_address'], int(x['value']))]
            else:
                return [(x['from_address'], -int(x['value']))]

        # Calc balance
        balances = (
            table_data
                .map(lambda record: json.loads(record[1]))
                .flatMap(split)
                .reduceByKey(lambda bal1, bal2: bal1+bal2)
                .map(lambda x: (x[0], (1.0*x[1])/(10**18)))
        )
        ```

3. shall에서 script 순차 실행하기

    - file: `./src/full-batch.sh`

    - full batch는 스크립트로 실행하고, daily batch는 airflow로 scheduling