# Daily Balance

## 1. Description

- Author: Jisung Lim 
- Contact: [email](iejisung@gmail.com), [Github](https://github.com/jisunglim)

## 2. Data Pipeline  Architecture

- ...


## 3. TODO

- daily batch 작성하기 (bq --fetch--> bq_temp --distinct--> gcs --insert--> mongoDB --> groupby)
    1. `ethereum_blockchain --> bq_temp`
    2. 
- daily batch airflow에 올리기
- full batch 마지막 부분 mongoDB에 넣는 걸로 수정하기
