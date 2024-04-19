# Databricks notebook source
dbutils.widgets.dropdown("Sample_Type", "sample_small", ["sample_small", "sample", "test", "all"])

# COMMAND ----------

spark.conf.set("spark.databricks.optimizer.adaptive.enabled", "false")
spark.conf.set('spark.worker.timeout', '600s')

# COMMAND ----------

import mosaic as mos
import pyspark.sql.functions as F
import json

mos.enable_mosaic(spark, dbutils)
mos.enable_gdal(spark)

# COMMAND ----------

# DBTITLE 1,CHANGE THIS to your schema
# MAGIC %sql
# MAGIC USE geospatial;

# COMMAND ----------

RESOLUTION = 7

# COMMAND ----------

sample = dbutils.widgets.getArgument("Sample_Type")

current_username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

with open(f"/Workspace/Users/{current_username}/dash-geospatial-hackathon-20240419/NY Flood/splits/{sample}.json", "r") as fp:
  sample_mukeys = json.load(fp)

df_sample_mukeys = spark.createDataFrame(sample_mukeys, "integer")\
  .withColumnRenamed("value", "mukey")

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil
# MAGIC
# MAGIC Column descriptions: https://www.nrcs.usda.gov/sites/default/files/2022-08/SSURGO-Metadata-Table-Column-Descriptions-Report.pdf
# MAGIC
# MAGIC Data model: https://www.nrcs.usda.gov/sites/default/files/2022-08/SSURGO-Data-Model-Diagram-Part-1_0_0.pdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component table

# COMMAND ----------

component_df = spark.read.table("component")\
  .join(df_sample_mukeys, on="mukey")

# COMMAND ----------

component_df.display()

# COMMAND ----------

numeric_components = [column for column, dtype in component_df.dtypes if dtype in ('double', 'float', 'integer', 'bigint', 'decimal') and column not in ('mukey', 'cokey')]
string_components = [column for column, dtype in component_df.dtypes if dtype == 'string' and column not in ('mukey', 'cokey')]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comonth table

# COMMAND ----------

comonth_df = spark.read.table("comonth")

# COMMAND ----------

# DBTITLE 1,Get average target values
comonth_df = comonth_df \
  .withColumn(
      "target", 
       F.when(F.col("flodfreqcl") == "Very frequent", 5)\
        .when(F.col("flodfreqcl") == "Frequent", 4)\
        .when(F.col("flodfreqcl") == "Occasional", 3)\
        .when(F.col("flodfreqcl") == "Rare", 2)\
        .when(F.col("flodfreqcl") == "Very rare", 1)\
        .otherwise(0)
    )
  
comonth_df = comonth_df.groupBy("cokey").agg(F.mean("target").alias("target"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join tables

# COMMAND ----------

target_df = comonth_df.join(component_df, on="cokey")

# COMMAND ----------

target_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map Unit Polygon table

# COMMAND ----------

mupolygon_df = spark.read.table("mupolygon")\
  .join(df_sample_mukeys, on="mukey")\
  .withColumn("geom", mos.st_updatesrid("Shape", "Shape_srid", F.lit(4326)))

# COMMAND ----------

mupolygon_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join onto the feature table

# COMMAND ----------

features_df = target_df.join(
  mupolygon_df.select("mukey", "geom"),
  on="mukey"
).cache()

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tessellation: an illustration

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC features_df "geom" "geometry" 1000

# COMMAND ----------

features_df_tessellated = features_df \
    .where(mos.st_isvalid("geom")) \
    .withColumn("grid", mos.grid_tessellateexplode("geom", F.lit(RESOLUTION))) \
    .withColumn("cell_id", F.col("grid.index_id")) \
    .withColumn("wkb", F.col("grid.wkb")) \
    .drop("grid")

# COMMAND ----------

features_df_grouped = features_df_tessellated.groupBy("cell_id").agg(
  F.first("wkb").alias("wkb"),
  *[F.mode(col).alias(col) for col in string_components],
  *[F.mean(col).alias(col) for col in numeric_components],
  F.mean("target").alias("target")
).cache()


# COMMAND ----------

features_df_grouped.display()

# COMMAND ----------

# MAGIC %%mosaic_kepler
# MAGIC features_df_grouped "wkb" "geometry" 1000

# COMMAND ----------

features_df_grouped.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(f"targets_{sample}")
