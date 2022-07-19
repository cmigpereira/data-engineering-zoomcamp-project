#!/usr/bin/env python
# coding: utf-8

import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

time_airflow = sys.argv[1]
date_time = datetime.strptime(time_airflow, '%Y-%m-%d %H:%M')

# Dataproc yarn master
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('coin_price') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BQ export data used by the connector.
bucket = "data_lake_cpereira-teste"
spark.conf.set('temporaryGcsBucket', bucket)

# Read from GCS
df = spark.read.parquet(f'gs://data_lake_cpereira-teste/raw/{date_time.strftime("%Y-%m-%d")}/{date_time.strftime("%H")}/')

# Aggregate by hour
df_hour = df.groupby('coin').agg(
        F.first("time").alias('time'),
        F.avg(F.col('high')).alias('avg_high_price'),
        F.avg(F.col('low')).alias('avg_low_price'),
        F.avg(F.col('open')).alias('avg_open_price'),
        F.first("currency").alias("currency")
        ).withColumn("time", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.col("time"),"yyyy-MM-dd HH:MM:ss"), "yyyy-MM-dd HH:00:00")))

# Reorder columns
df_hour_reordered = df_hour.select('time', 'coin', 'avg_high_price', 'avg_low_price', 'avg_open_price', 'currency')

# Write to BQ
df_hour_reordered.write \
  .format("bigquery") \
  .mode("append") \
  .option('project','cpereira-teste') \
  .option("partitionField", "time") \
  .option("clusteredFields", "coin") \
  .option('dataset','coin_price_production') \
  .option('table','coin_price_hourly') \
  .save()