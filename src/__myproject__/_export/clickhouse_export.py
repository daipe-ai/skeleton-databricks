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

# MAGIC %md ### Set Checkpoint Dir

# COMMAND ----------

@dp.notebook_function("%daipeproject.checkpoint.dir%")
def set_checkpoint_dir(checkpoint_dir: str, spark: SparkSession):
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

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
    ]).replace(float('nan'), None).checkpoint()

# COMMAND ----------

@dp.transformation(features_to_export_with_conversions, display=False)
def sampled_features(df: DataFrame):
    return df.sample(0.01)

# COMMAND ----------

def count_percentile(col: str, percentile_percentage: float, accuracy: int) -> Column:
    return f.percentile_approx(f.when(f.col(col) > 0, f.col(col)), percentile_percentage, accuracy)

def round_bin(col: str, current_bin: int, bin_count: int) -> Column:
    return current_bin * f.col(f"{col}_quantile") / (bin_count - 1)

def make_bin_array(col: str, bin_count: int) -> Column:
    return f.array(
        *(round_bin(col, i, bin_count - 1) for i in range(bin_count - 1)),
        f.col(f"{col}_max")
    )

def make_bin_string(col: str, bin_count: int) -> Column:
    return f.concat_ws("-", make_bin_array(col, bin_count))

# COMMAND ----------

@dp.transformation(
    features_to_export_with_conversions,
    "%daipeproject.export.bins%",
    display=False
)
def floating_point_number_bins(df: DataFrame, bin_params: Box):
    columns = [column for column, dtype in df.dtypes if dtype in ("float", "double")]

    return df.select(
        *(count_percentile(col, bin_params.higher_percentile_percentage, bin_params.accuracy).alias(f"{col}_quantile") for col in columns),
        *(f.max(col).alias(f"{col}_max") for col in columns)
     ).select(
        *(make_bin_string(col, bin_params.bin_count).alias(col) for col in columns)
    )

# COMMAND ----------

def get_low_quantiles(df: DataFrame, columns: list, lower_percentile_percentage: float, accuracy: int) -> dict:
    low_quantiles = df.select(
        *[f.percentile_approx(f.col(col), lower_percentile_percentage, accuracy).alias(col) for col in columns]
    ).collect()[0].asDict()

    return low_quantiles

def get_high_quantiles(df: DataFrame, columns: list, higher_percentile_percentage: float, accuracy: int, low_quantiles: dict) -> dict:
    high_quantiles = df.select(
        *[(f.percentile_approx(f.when(f.col(col) > low_quantiles[col], f.col(col)), higher_percentile_percentage, accuracy) + 1).alias(col) for col in columns]
    ).collect()[0].asDict()

    for key, value in high_quantiles.items():
        if value is None:
            high_quantiles[key] = low_quantiles[key] + 1

    return high_quantiles

def get_distinct_bins(col: str) -> Column:
    return f.collect_set(col)

def remove_quantiles_if_bin_count_exceeds_threshold(col: str, bin_count: int, low_quantile: int, high_quantile: int) -> Column:
    return (
        f.when(f.size(col) <= bin_count, f.col(col))
        .otherwise(f.filter(f.col(col), lambda x: (x >= low_quantile) & (x <= high_quantile)))
    )

def generate_linear_bins_if_bin_count_exceeds_threshold(col: str, bin_count: int, low_quantile: int, high_quantile: int) -> Column:
    return (
        f.when(f.size(col) <= bin_count, f.array_sort(f.col(col)))
        .otherwise(f.array(*map(f.lit, sorted(np.round(np.linspace(low_quantile, high_quantile, bin_count))))))
    )

# COMMAND ----------

@dp.transformation(
    features_to_export_with_conversions,
    "%daipeproject.export.bins%",
    display=False
)
def integral_number_bins(df: DataFrame, bin_params: Box):
    columns = [column for column, dtype in df.dtypes if dtype in ("int", "bigint")]
    low_quantiles = get_low_quantiles(df, columns, bin_params.lower_percentile_percentage, bin_params.accuracy)
    high_quantiles = get_high_quantiles(df, columns, bin_params.higher_percentile_percentage, bin_params.accuracy, low_quantiles)

    return (
        df
        .select(
            *(
                get_distinct_bins(col).alias(col)
                for col in columns
              )
        ).select(
            *(
                remove_quantiles_if_bin_count_exceeds_threshold(col, bin_params.bin_count, low_quantiles[col], high_quantiles[col])
                .alias(col)
                for col in columns
            )
        )
        .select(
            *(
                generate_linear_bins_if_bin_count_exceeds_threshold(col, bin_params.bin_count, low_quantiles[col], high_quantiles[col])
                .alias(col)
                for col in columns
            )
        ).select(
            *(
                f.concat_ws("-", col).alias(col)
                for col in columns
            )
        )
    )

# COMMAND ----------

@dp.transformation(floating_point_number_bins, integral_number_bins, display=False)
def generate_bins(fp_bins: DataFrame, int_bins: DataFrame):
    return fp_bins.join(int_bins, how="outer")

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

# MAGIC %md ### Clean Checkpoint Dir

# COMMAND ----------

@dp.notebook_function()
def clean_checkpoint_dir(spark: SparkSession, dbutils: DBUtils):
    dbutils.fs.rm(spark.sparkContext.getCheckpointDir(), recurse=True)

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
