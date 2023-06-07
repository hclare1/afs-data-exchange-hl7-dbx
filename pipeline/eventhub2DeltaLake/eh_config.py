# Databricks notebook source
# MAGIC %run ../common/setup_env

# COMMAND ----------
from pyspark.sql.functions import current_timestamp

def transferEventHubDataToLake(eventHubConfig, lakeConfig, topic):
    ehConfig = eventHubConfig.getConfig(topic)
    df = spark.readStream.format("eventhubs").options(**ehConfig).load()
    df = df.withColumn("body", df["body"].cast("string")).withColumn("_created_timestamp", current_timestamp()})
    
    # Standardize on Table names for Event Hub topics:
    tbl_name = normalizeString(topic) + "_eh_raw"
    
    lakeDAO = LakeDAO(lakeConfig)
    lakeDAO.writeStreamTo(df, tbl_name)

