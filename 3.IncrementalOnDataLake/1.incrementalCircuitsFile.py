# Databricks notebook source
# MAGIC %md
# MAGIC #### DEFINE SCHEMA FOR circuits.csv FILE

# COMMAND ----------

# MAGIC %run "../9.Includes/1.config"

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE NAME

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %md
# MAGIC #### PASS THE PARAMETER FOR THE FILE DATE

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-04-18")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

print(raw_path)
print(v_file_date)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, FloatType, DoubleType

circuits_schema = StructType(fields = 
 [
  StructField("circuitId", IntegerType(), True),
  StructField("circuitRef", StringType(), True),
  StructField("name", StringType(), True),
  StructField("location", StringType(), True),
  StructField("country", StringType(), True),
  StructField("lat", DoubleType(), True),
  StructField("lng", DoubleType(), True),
  StructField("alt", DoubleType(), True),
  StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### INGEST circuits.csv FILE

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_path}/incremental/{v_file_date}/circuits.csv")

display(circuits_df)
circuits_df.printSchema()
print(f"Number of Records Read {circuits_df.count()}")
print(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SELECT ONLY THE REQUIRED COLUMNS

# COMMAND ----------

from pyspark.sql.functions import col, lit
sel_circuits_df = circuits_df.select(
                                     col("circuitId").alias("circuit_id"), 
                                     col("circuitRef").alias("circuit_ref"),
                                     col("name"), "location", col("country"), 
                                     col("lat"), col("lng"), col("alt")
                                    )
display(sel_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### RENAME THE COLUMNS AS REQUIRED

# COMMAND ----------

rename_circuits_df = sel_circuits_df.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_name", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

display(rename_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ADD NEW COLUMNS

# COMMAND ----------

# MAGIC %run "../9.Includes/2.functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
# circuits_final_df = rename_circuits_df.withColumn("load_dtm", current_timestamp())
circuits_final_df = ingest_dtm(rename_circuits_df)

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### WRITE DATA TO THE DATALAKE AS PARQUET FILE

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{incremental_path}/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC #### READ THE DATA WE WROTE TO DATALAKE BACK INTO A DATAFRAME TO PROVE THE WRITE WORKED

# COMMAND ----------

# validate_circuits_df = spark.read \
# .parquet(f"{incremental_path}/circuits")

# display(validate_circuits_df)
# validate_circuits_df.printSchema()
# print(f"Number of Records Read {validate_circuits_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### REPLICATE THE CIRCUITS DATA INSIDE INCREMENTAL DB

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_incremental.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.circuits;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as cnt FROM f1_incremental.circuits;

# COMMAND ----------

dbutils.notebook.exit("INCREMENTAL LOAD FOR CIRCUITS HAS BEEN LOADED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_incremental.circuits LIMIT 10;
