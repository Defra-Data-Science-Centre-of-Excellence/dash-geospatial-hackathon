# Databricks notebook source
dbutils.widgets.dropdown("Sample_Type", "sample_small", ["sample_small", "sample", "test", "all"])

# COMMAND ----------

sample = dbutils.widgets.getArgument("Sample_Type")

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import os
mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG marcell;
# MAGIC USE SCHEMA geospatial;

# COMMAND ----------

feature_table = spark.read.table(f"features_{sample}")

# COMMAND ----------

feature_table.display()

# COMMAND ----------

# DBTITLE 1,Create hexagons from cell IDs
feature_table = feature_table \
  .withColumn("wkb", mos.grid_boundaryaswkb("cell_id"))

# COMMAND ----------

# DBTITLE 1,Visualise with Kepler
# MAGIC %%mosaic_kepler
# MAGIC feature_table "wkb" "geometry" 10000
