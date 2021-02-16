# Processing ACLED Data using Pyspark & Dataproc 

### Using pyspark, I wanted to work with google cloud's dataproc compute engine and simulate processing a "large" csv file

### Steps within Google Cloud Shell
1. Enable Google DataProc: 

```gcloud services enable dataproc.googleapis.com``` 

2. Create a BigQuery dataset to write the tables to:
```
bq --location=us-west1 mk -d \
    --default_table_expiration=3600 \
    --description "my acled pyspark processing output dataset" \
    acled_dataset
```

3. Create DataProc cluster: 
```
gcloud dataproc clusters create acled-pyspark-processing \
    --region=us-west1 \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --temp-bucket=acled-pyspark-bucket
```

4. Copy PySpark job file from Google Cloud Storage into current terminal session:

```gsutil cp -r gs://${BUCKET-NAME}/acled_pyspark_analytics.py .```

5. Submit PySpark job to DataProc: 

```
gcloud dataproc jobs submit pyspark acled_pyspark_analytics.py \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
    --cluster acled-pyspark-processing \
    --region=us-west1 \
    -- gs://acled-pyspark-bucket/2020-01-01-2020-12-31.csv
```
