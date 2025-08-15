# Databricks notebook source
# MAGIC %md
# MAGIC LIVE IOT DATA TO DELTA LOCATION

# COMMAND ----------

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import time
import random

# Start Spark session
spark = SparkSession.builder.appName("IoT_Data_Generator").getOrCreate()

# Delta path (adjust to your mount path)
delta_path = "dbfs:/user/hive/warehouse/iot_events"

# Loop to generate data every 2 seconds
for i in range(50):  # Generates 50 batches (~100 seconds)
    # Generate Python timestamp

    # event_time = datetime.now()
    
    # Create a Python list of data
    data = [
        (
            i,  # event ID
            f"device_{random.randint(1, 5)}",  # device_id
            round(random.uniform(20.0, 35.0), 2),  # temperature
            round(random.uniform(30.0, 70.0), 2)  # humidity
            # event_time  # event_time as Python datetime
        )
    ]
    
    # Create DataFrame with explicit schema
    df = spark.createDataFrame(
        data,
        ["event_id", "device_id", "temperature", "humidity"]
        # ["event_id", "device_id", "temperature", "humidity", "event_time"]
    )
    
    # Append to Delta table
    df.write.format("delta").mode("append").save(delta_path)
    
    print(f"Inserted batch {i+1} at {datetime.now().strftime('%H:%M:%S')}")
    time.sleep(20)

spark.stop()


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/user/hive/warehouse/iot_events`

# COMMAND ----------

# MAGIC %md
# MAGIC ## APPLY WATER MARKING TO AGG COUNT WITH LIVE TRANSFORMATION 

# COMMAND ----------

from pyspark.sql.functions import window, col

# Read streaming data from Delta table
df = spark.readStream.format("delta").load("dbfs:/user/hive/warehouse/iot_events")

# Add processing time as a columnfor water marking
df = df.withColumn("proc_time", current_timestamp())

# Apply watermark and aggregate by device in 1-min windows
agg_df = (
    df
    .withWatermark("proc_time", "2 minutes")  # Max lateness allowed
    .groupBy(
        window(col("proc_time"), "1 minute"),  # Tumbling window
        col("device_id")
    )
    .count()
)



# COMMAND ----------

agg_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Output results to console for testing

# COMMAND ----------

# data save on delta with proc_time 
df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation","dbfs:/user/hive/warehouse/iot_event_data/checkpoints/data_checkpoint")\
    .start("dbfs:/user/hive/warehouse/iot_event_data/")
    # .table("dbfs:/user/hive/warehouse/iot_event_data/")

    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/user/hive/warehouse/iot_event_data/` ORDER BY proc_time DESC
# MAGIC
# MAGIC

# COMMAND ----------


#agg data save in delta    
agg_df \
    .coalesce(5) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/hive/warehouse/iot_event_WM_agg/checkpoints/iot_agg") \
    .start("dbfs:/user/hive/warehouse/iot_event_WM_agg/")
    # .table("dbfs:/user/hive/warehouse/iot_event_delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/iot_event_WM_agg/
