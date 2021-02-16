#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import types
from pyspark.ml.feature import StopWordsRemover, Tokenizer

if len(sys.argv) != 2:
    print("Number of arguments: ", len(sys.argv))
    raise Exception("Exactly ONE pyspark job file is required!")

PATH = sys.argv[-1]

spark = SparkSession.builder.appName("acledPyspark").getOrCreate()
df = spark.read.csv(
    path=PATH, 
    sep=",",
    quote='"',
    header=True
)
print(df.count(), len(df.columns))
df.printSchema()

# Column transformations 
df = df.withColumn("event_date", f.to_date("event_date", "dd MMMM yyyy"))
df = df.withColumn("year", f.to_date("year", "yyyy"))
df = df.withColumn("latitude", df["latitude"].cast(types.DoubleType()))
df = df.withColumn("longitude", df["longitude"].cast(types.DoubleType()))
df = df.withColumn("fatalities", df["fatalities"].cast(types.IntegerType()))


# WRITING TABLES TO BQ 
bucket = 'acled-pyspark-bucket'  
spark.conf.set('temporaryGcsBucket', bucket)

# 1. write historic parquet file into bigquery (more compact)
df.write.format("bigquery") \
    .option('table', 'acled_dataset.historic_fact_table') \
    .save()

# 2. events per day 2020 
events_per_day = df.select("event_date", "year") \
    .groupBy("event_date", "year") \
    .agg(
        f.count("*").alias("events_per_day")
    ) \
    .select("event_date", "events_per_day") \
    .sort("event_date")
events_per_day.write.format("bigquery") \
    .option('table', 'acled_dataset.events_per_day') \
    .save()

# 3. Fatalities 
fatalities = df.groupBy("event_date").agg(
    f.sum("fatalities").alias("fatalities_per_day")
).sort("event_date")
fatalities.write.format("bigquery") \
    .option('table', 'acled_dataset.fatalities_per_day') \
    .save()

fatalities_by_country = df.select("country", "fatalities") \
    .groupBy(df["country"]) \
    .agg(
        f.sum(df["fatalities"]).alias("fatalities_per_country")
    ) \
    .select("country", "fatalities_per_country") 
fatalities_by_country.write.format("bigquery") \
    .option('table', 'acled_dataset.fatalities_by_country') \
    .save()

spark.stop()