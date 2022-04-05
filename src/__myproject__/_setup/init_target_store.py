# Databricks notebook source
# MAGIC %md
# MAGIC # Create target store and enum tables

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

import daipe as dp

from logging import Logger
from pyspark.sql import types as t, SparkSession

from featurestorebundle.db.TableNames import TableNames

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create_for_entity()

# COMMAND ----------

entity = dp.fs.get_entity()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create target store table
# MAGIC Schema: `entity_id_column_name entity_id_column_type, timestamp timestamp, target_id string`

# COMMAND ----------

targets_table_schema = t.StructType(
    [
        t.StructField(entity.id_column, entity.id_column_type, False),
        t.StructField(entity.time_column, entity.time_column_type, False),
        t.StructField("target_id", t.StringType(), False),
    ]
)

# COMMAND ----------

@dp.notebook_function("%featurestorebundle.target%", "%featurestorebundle.db_name%")
def init_targets_table(target_config, db_name, logger: Logger, table_names: TableNames, spark: SparkSession):
    targets_path = table_names.get_targets_path(entity.name)
    targets_table = table_names.get_targets_table_name(entity.name)
    logger.info("target store schema: " + str(targets_table_schema))
    
    write = spark.createDataFrame([], schema=targets_table_schema).write.option("path", targets_path).format("delta")
    
    if target_config.backend == "delta_table":
        write.saveAsTable(f"{db_name}.{targets_table}")
    else:
        write.save()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create target store table
# MAGIC Schema: `target_id string, description string`

# COMMAND ----------

targets_enum_table_schema = t.StructType(
    [
        t.StructField("target_id", t.StringType(), False),
        t.StructField("description", t.StringType(), True),
    ]
)

# COMMAND ----------

@dp.notebook_function("%featurestorebundle.target%", "%featurestorebundle.db_name%")
def init_targets_enum_table(target_config, db_name, logger: Logger, table_names: TableNames, spark: SparkSession):
    targets_enum_path = table_names.get_targets_enum_path()
    targets_enum_table = table_names.get_targets_enum_table_name()
    logger.info("target store enum schema: " + str(targets_enum_table_schema))
    spark.createDataFrame([], schema=targets_enum_table_schema).write.format("delta").save(targets_enum_path)
    
    write = spark.createDataFrame([], schema=targets_enum_table_schema).write.option("path", targets_enum_path + "1").format("delta")
    
    if target_config.backend == "delta_table":
        write.saveAsTable(f"{db_name}.{targets_enum_table}")
    else:
        write.save()
