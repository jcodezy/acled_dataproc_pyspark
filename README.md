# Processing ACLED Data using Pyspark & Dataproc 

### Using pyspark, I wanted to work with google cloud's dataproc compute engine and simulate a batch processing of a "large" csv file

__Taken from ACLED:__ "ACLED collects real-time data on the locations, dates, actors, fatalities, and types of all reported political violence and protest events across Africa, the Middle East, Latin America & the Caribbean, East Asia, South Asia, Southeast Asia, Central Asia & the Caucasus, Europe, and the United States of America."  
LINK for more info: https://acleddata.com/#/dashboard  

##### Data is from January 1st, 2020 through to December 31st, 2020
Dataset file size: 133 mb 


### Steps within Google Cloud Shell
#### 1. Enable Google DataProc: 

```gcloud services enable dataproc.googleapis.com``` 

#### 2. Create a BigQuery dataset to write the tables to:
```
bq --location=us-west1 mk -d \
    --default_table_expiration=3600 \
    --description "my acled pyspark processing output dataset" \
    acled_dataset
```

#### 3. Create DataProc cluster: 
```
gcloud dataproc clusters create acled-pyspark-processing \
    --region=us-west1 \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --temp-bucket=acled-pyspark-bucket
```

#### 4. Copy PySpark job file from Google Cloud Storage into current terminal session:

```gsutil cp -r gs://${BUCKET-NAME}/acled_pyspark_analytics.py .```

#### 5. Submit PySpark job to DataProc: 

```
gcloud dataproc jobs submit pyspark acled_pyspark_analytics.py \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest.jar \
    --cluster acled-pyspark-processing \
    --region=us-west1 \
    -- gs://acled-pyspark-bucket/2020-01-01-2020-12-31.csv
```

### BigQuery result
![bigquery output](https://github.com/jcodezy/acled_dataproc_pyspark/blob/master/assets/bigquery_output.png)

![bigquery events_per_day](https://github.com/jcodezy/acled_dataproc_pyspark/blob/master/assets/events_per_day.png)
