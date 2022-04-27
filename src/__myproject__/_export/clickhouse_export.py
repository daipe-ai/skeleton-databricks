# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Imports

# COMMAND ----------

import os
import requests
import json
import daipe as dp
import numpy as np
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.dbutils import DBUtils
from logging import Logger

# COMMAND ----------

# MAGIC %md ### Get parameters

# COMMAND ----------

entity = dp.fs.get_entity()

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config_url", "", "Config url")

# COMMAND ----------

@dp.notebook_function(dp.get_widget_value("config_url"))
def load_config(config_url: str, logger: Logger, spark: SparkSession):
    config = spark.read.json(config_url).collect()[0].asDict()
    logger.info(f"Loaded config from {config_url}")
    return config

# COMMAND ----------

@dp.notebook_function(os.environ["APP_ENV"])
def get_table_names(current_env: str):
    return {
        "features_main": f"featurestore_{current_env}",
        "features_temp": f"featurestore_{current_env}_temp",
        "features_backup": f"featurestore_{current_env}_backup",
        "sampled_main": f"sampled_featurestore_{current_env}",
        "sampled_temp": f"sampled_featurestore_{current_env}_temp",
        "sampled_backup": f"sampled_featurestore_{current_env}_backup",
        "bins": f"bins_{current_env}",
    }

# COMMAND ----------

@dp.notebook_function()
def get_secrets(dbutils: DBUtils):
    return {
        "user": dbutils.secrets.get(scope="unit-kv", key="clickhouse-user"),
        "pass": dbutils.secrets.get(scope="unit-kv", key="clickhouse-pass"),
    }

# COMMAND ----------

@dp.notebook_function("%daipeproject.clickhouse.host%", "%daipeproject.clickhouse.port%")
def get_clickhouse_address(host: str, port: int):
    return {
        "host": host,
        "port": port,
    }

# COMMAND ----------

# MAGIC %md ### Methods to access Clickhouse

# COMMAND ----------

def execute_clickhouse_query(query: str):
    response = requests.post(
        f"http://{get_clickhouse_address.result['host']}:{get_clickhouse_address.result['port']}/",
        params={"user": get_secrets.result["user"], "password": get_secrets.result["pass"], "default_format": "JSON"},
        data=query)
    return json.loads(response.content.decode("utf8") or "{}").get("data")

# COMMAND ----------

def upload_table_to_clickhouse(df: DataFrame, table_name: str, engine_type: str):
    """
    Uploads a dataframe to Clickhouse database configured for this project.
    df: Dataframe to upload
    table_name: Name of the destination table
    engine_type: Type of engine for the table. One of: summing_merge_tree, aggregating_merge_tree, log
    """

    engine_map = {
        "summing_merge_tree": f"ENGINE = SummingMergeTree() ORDER BY {entity.id_column}",
        "aggregating_merge_tree": f"ENGINE = AggregatingMergeTree() ORDER BY {entity.id_column}",
        "log": "ENGINE = Log",
    }
    (df.write
        .format("jdbc")
        .option("createTableOptions", engine_map[engine_type])
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
        .option("url", f"jdbc:clickhouse://{get_clickhouse_address.result['host']}:{get_clickhouse_address.result['port']}")
        .option("dbtable", table_name)
        .option("user", get_secrets.result["user"])
        .option("password", get_secrets.result["pass"])
        .mode('Overwrite')
        .save())

# COMMAND ----------

def move_temp_table_to_main_and_backup(temp_table_name: str, main_table_name: str, backup_table_name: str):
    execute_clickhouse_query(f"DROP TABLE IF EXISTS {backup_table_name}")

    if execute_clickhouse_query(f"EXISTS {main_table_name}")[0]["result"]:
        execute_clickhouse_query(f"RENAME TABLE {main_table_name} TO {backup_table_name}")

    execute_clickhouse_query(f"RENAME TABLE {temp_table_name} TO {main_table_name}")

# COMMAND ----------

# MAGIC %md ### Check ongoing export

# COMMAND ----------

@dp.notebook_function(get_table_names)
def check_ongoing_export(table_names: dict, dbutils: DBUtils, logger: Logger):
    if execute_clickhouse_query(f"EXISTS {table_names['features_temp']}")[0]["result"]:
        logger.error("Clickhouse export is already ongoing in this environment.")
        dbutils.notebook.exit(0)

# COMMAND ----------

# MAGIC %md ### Create tables

# COMMAND ----------

@dp.transformation(load_config, display=False)
def features_to_export(config: dict, feature_store: dp.fs.FeatureStore):
    features = config["params"].export_columns
    return feature_store.get_latest(entity.name, features=features)

# COMMAND ----------

@dp.transformation(features_to_export, display=False)
def features_to_export_with_conversions(df: DataFrame):
    return df.select(*[
        f.col(c).cast("float") if ("double" in t or "decimal" in t) else f.col(c) for c, t in df.dtypes
    ]).replace(float('nan'), None)

# COMMAND ----------

@dp.transformation(features_to_export_with_conversions, display=False)
def sampled_features(df: DataFrame):
    return df.sample(0.01)

# COMMAND ----------

@dp.transformation(features_to_export_with_conversions, display=False)
def generate_bins(df: DataFrame, spark: SparkSession):
    numerical = [column for column, type in df.dtypes if type in ("float", "int", "double", "bigint")]
    bins_columns = []
    bins_data = []

    for col in numerical:
        bins_columns.append(StructField(col, StringType(), True))

        filtered_df = df.filter(f.col(col) > 0).select(f.percentile_approx(col, 0.963), f.max(col))

        quantile = filtered_df.collect()[0][0] or 0.01
        maximum  = filtered_df.collect()[0][1] or 1

        bins = np.arange(0, quantile * 1.1, quantile / 9)
        bins[-1] = maximum
        if bins[0] != 0:
            bins.insert(0, 0)

        bins_data.append("-".join(np.around(bins, 3).astype(str)))

    return spark.createDataFrame(data=[bins_data], schema=StructType(bins_columns))

# COMMAND ----------

# MAGIC %md ### Write tables

# COMMAND ----------

@dp.notebook_function(features_to_export_with_conversions, get_table_names)
def write_features(df: DataFrame, table_names: dict, logger: Logger):
    logger.info("Writing features to ClickHouse database.")
    upload_table_to_clickhouse(df, table_names["features_temp"], "aggregating_merge_tree")

# COMMAND ----------

@dp.notebook_function(sampled_features, get_table_names)
def write_sampled_features(df: DataFrame, table_names: dict, logger: Logger):
    logger.info("Writing sampled features to ClickHouse database.")
    upload_table_to_clickhouse(df, table_names["sampled_temp"], "aggregating_merge_tree")

# COMMAND ----------

@dp.notebook_function(generate_bins, get_table_names)
def write_bins(df: DataFrame, table_names: dict, logger: Logger):
    logger.info("Writing feature bins to ClickHouse database.")
    upload_table_to_clickhouse(df, table_names["bins"], "log")

# COMMAND ----------

@dp.notebook_function(get_table_names)
def rename_tables(table_names: dict, logger: Logger):
    move_temp_table_to_main_and_backup(
        table_names['features_temp'],
        table_names['features_main'],
        table_names['features_backup'],
    )
    logger.info(f"Features export successful, table saved as {table_names['features_main']}, backup saved as {table_names['features_backup']}.")

    move_temp_table_to_main_and_backup(
        table_names['sampled_temp'],
        table_names['sampled_main'],
        table_names['sampled_backup'],
    )
    logger.info(f"Sampled features export successful, table saved as {table_names['sampled_main']}, backup saved as {table_names['sampled_backup']}.")

# COMMAND ----------

# MAGIC %md ### Checks

# COMMAND ----------

@dp.notebook_function(features_to_export_with_conversions, get_table_names)
def compare_counts(df: DataFrame, table_names: dict, logger: Logger):
    fs_count = df.count()
    clickhouse_count = int(execute_clickhouse_query(f"SELECT COUNT(*) from {table_names['features_main']}")[0]["count()"])
    if fs_count == clickhouse_count:
        logger.info(f"Featurestore row count equals to Clickhouse export row count ({fs_count})")
    else:
        raise Exception(f"Featurestore row count is not equal to export row count. Featurestore: {fs_count} | Clickhouse: {clickhouse_count}")
