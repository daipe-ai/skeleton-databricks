# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md ### Imports

# COMMAND ----------

import os
import requests
import json
import daipe as dp
from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as f
from pyspark.dbutils import DBUtils
from box import Box
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

@dp.notebook_function("%daipeproject.export.clickhouse%")
def get_clickhouse_address(clickhouse: Box):
    return clickhouse

# COMMAND ----------

# MAGIC %md ### Methods to access Clickhouse

# COMMAND ----------

def execute_clickhouse_query(query: str):
    response = requests.post(
        f"http://{get_clickhouse_address.result.host}:{get_clickhouse_address.result.port}/",
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
        .option("url", f"jdbc:clickhouse://{get_clickhouse_address.result.host}:{get_clickhouse_address.result.port}")
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

def count_percentile(col: str, percentile_percentage: float) -> Column:
    return f.percentile_approx(f.when(f.col(col) > 0, f.col(col)), percentile_percentage)

def round_bin(col: str, current_bin: int, bin_count: int, round_scale: int) -> Column:
    return f.round(current_bin * f.col(f"{col}_quantile") / bin_count - 1, round_scale)

def make_bin_array(col: str, bin_count: int, round_scale: int) -> Column:
    return f.array(
        *(round_bin(col, i, bin_count - 1, round_scale) for i in range(bin_count - 1)),
        f.col(f"{col}_max")
    )

def make_bin_string(col: str, bin_count: int, round_scale: int) -> Column:
    return f.concat_ws("-", make_bin_array(col, bin_count, round_scale))

# COMMAND ----------

@dp.transformation(
    features_to_export_with_conversions,
    "%daipeproject.export.bins%",
    display=False
)
def generate_bins(df: DataFrame, bin_params: Box):
    numerical_columns = [column for column, dtype in df.dtypes if dtype in ("float", "int", "double", "bigint")]

    return df.select(
        *(count_percentile(col, bin_params.percentile_percentage).alias(f"{col}_quantile") for col in numerical_columns),
        *(f.max(col).alias(f"{col}_max") for col in numerical_columns)
    ).select(
        *(make_bin_string(col, bin_params.bin_count, bin_params.round_scale).alias(col) for col in numerical_columns)
    )

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
