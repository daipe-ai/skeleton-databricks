# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import os
import requests
import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.dbutils import DBUtils
from logging import Logger
import daipe as dp

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

entity = dp.fs.get_entity()

# COMMAND ----------

@dp.notebook_function(os.environ["APP_ENV"])
def get_table_names(current_env: str):
    return {
        "main": f"featurestore_{current_env}",
        "temp": f"featurestore_{current_env}_temp",
        "backup": f"featurestore_{current_env}_backup",
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

def execute_clickhouse_query(query: str):
    response = requests.post(
        f"http://{get_clickhouse_address.result['host']}:{get_clickhouse_address.result['port']}/",
        params={"user": get_secrets.result["user"], "password": get_secrets.result["pass"], "default_format": "JSON"},
        data=query)
    return json.loads(response.content.decode("utf8") or "{}").get("data")

# COMMAND ----------

@dp.notebook_function(get_table_names)
def check_ongoing_export(table_names: dict, dbutils: DBUtils, logger: Logger):
    if execute_clickhouse_query(f"EXISTS {table_names['temp']}")[0]["result"]:
        logger.error("Clickhouse export is already ongoing in this environment.")
        dbutils.notebook.exit(0)

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

@dp.transformation(features_to_export_with_conversions, get_table_names, get_secrets, "%daipeproject.clickhouse.host%", "%daipeproject.clickhouse.port%")
def write_features(df: DataFrame, table_names: dict, secrets: dict, clickhouse_host: str, clickhouse_port: int, logger: Logger):
    logger.info(f"Writing features to ClickHouse database at {clickhouse_host}:{clickhouse_port}")
    (df.write
        .format("jdbc")
        .option("createTableOptions", "ENGINE = SummingMergeTree() ORDER BY client_id")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("url", f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}")
        .option("dbtable", table_names["temp"])
        .option("user", secrets["user"])
        .option("password", secrets["pass"])
        .mode('Overwrite')
        .save())

# COMMAND ----------

@dp.notebook_function(get_table_names)
def rename_tables(table_names: dict, logger: Logger):
    execute_clickhouse_query(f"DROP TABLE IF EXISTS {table_names['backup']}")

    if execute_clickhouse_query(f"EXISTS {table_names['main']}")[0]["result"]:
        execute_clickhouse_query(f"RENAME TABLE {table_names['main']} TO {table_names['backup']}")
        logger.info(f"Table {table_names['backup']} updated with data from {table_names['main']}.")

    execute_clickhouse_query(f"RENAME TABLE {table_names['temp']} TO {table_names['main']}")

    logger.info(f"Export successful, table saved as {table_names['main']}.")
