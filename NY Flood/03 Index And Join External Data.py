# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

dbutils.widgets.dropdown("Sample_Type", "sample_small", ["sample_small", "sample", "test", "all"])

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

# DBTITLE 1,CHANGE THIS to your schema
# MAGIC %sql
# MAGIC USE geospatial;

# COMMAND ----------

RESOLUTION = 7

# COMMAND ----------

sample = dbutils.widgets.getArgument("Sample_Type")

# COMMAND ----------

# MAGIC %md
# MAGIC # Soil

# COMMAND ----------

features_df = spark.read.table(f"targets_{sample}")\
  .select("cell_id", "target", "drainagecl", "corsteel", "corcon", "slope_l", "slope_h", "slope_r")

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Weather Data

# COMMAND ----------

weather_df = spark.read.table("weather")

# COMMAND ----------

features_df = features_df.join(
  weather_df,
  on="cell_id",
  how="left"
)

# COMMAND ----------

features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Hydrography Data

# COMMAND ----------

df_nhda = spark.read.table("hydorgraphy_area")\
  .where(mos.st_numpoints("geom_0")<5000)
  

# COMMAND ----------

df_nhda = df_nhda\
  .repartition(8)\
  .withColumn("grid",
              mos.grid_tessellateexplode(
                  mos.st_buffer("geom_0", F.lit(0)), F.lit(RESOLUTION))
              )\
  .withColumn("cell_id", F.col("grid.index_id"))\
  .withColumn("wkb", F.col("grid.wkb"))
  

# COMMAND ----------

df_nhda_grouped = df_nhda.groupBy("cell_id").agg(
  F.mean("visibility").alias("visibility"),
  F.mean("elevation").alias("elevation")
)

# COMMAND ----------

features_df = features_df.join(
  df_nhda_grouped,
  on="cell_id",
  how="left"
).cache()

# COMMAND ----------

# MAGIC %md
# MAGIC # Roads

# COMMAND ----------

roads_df = spark.read.table("roads")\
  .withColumn("grid", mos.grid_tessellateexplode("geom_0", F.lit(RESOLUTION)))\
  .drop("geom_0")\
  .withColumn("geom", F.col("grid.wkb"))\
  .where(mos.st_isvalid("geom"))\
  .withColumn("cell_id", F.col("grid.index_id"))


# COMMAND ----------

roads_df = roads_df\
  .withColumn("road_length", mos.st_length("geom"))

# COMMAND ----------

features_df = features_df.join(
  roads_df.select("cell_id", "road_length"),
  on="cell_id",
  how="left"
)

# COMMAND ----------

features_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"features_{sample}")
