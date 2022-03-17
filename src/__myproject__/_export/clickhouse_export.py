# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import os
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.dbutils import DBUtils
from clickhouse_driver import Client as ClickhouseClient
from logging import Logger
import daipe as dp

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

@dp.notebook_function("%odapfeatures.clickhouse.host%", get_secrets)
def clickhouse_connection(clickhouse_host: str, secrets: dict):
    return ClickhouseClient(
        clickhouse_host,
        user=secrets["user"],
        password=secrets["pass"]
    )

# COMMAND ----------

@dp.notebook_function(clickhouse_connection, get_table_names)
def check_ongoing_export(clickhouse: ClickhouseClient, table_names: dict, dbutils: DBUtils, logger: Logger):
    existing_tables = clickhouse.execute("SHOW TABLES")
    if (table_names["temp"],) in existing_tables:
        logger.error(f"Clickhouse export is already ongoing in this environment.")
        dbutils.notebook.exit(0)

# COMMAND ----------

@dp.transformation("%odapfeatures.clickhouse.excluded_columns%", display=False)
def features_to_export(excluded_columns, feature_store: dp.fs.FeatureStore):
    return feature_store.get_latest(entity.name).drop(*excluded_columns)

# COMMAND ----------

@dp.transformation(features_to_export, display=False)
def features_to_export_with_conversions(df: DataFrame):
    return df.select(*[
        f.col(c).cast("float") if ("double" in t or "decimal" in t) else f.col(c) for c, t in df.dtypes
    ])

# COMMAND ----------

@dp.transformation(features_to_export_with_conversions, get_table_names, get_secrets, "%odapfeatures.clickhouse.host%", "%odapfeatures.clickhouse.port%")
def write_features(df: DataFrame, table_names: dict, secrets: dict, clickhouse_host: str, clickhouse_port: int, dbutils: DBUtils, logger: Logger):
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

@dp.notebook_function(clickhouse_connection, get_table_names)
def rename_tables(clickhouse: ClickhouseClient, table_names: dict, logger: Logger):
    existing_tables = clickhouse.execute("SHOW TABLES")
    clickhouse.execute(f"DROP TABLE IF EXISTS {table_names['backup']}")

    if (table_names['main'],) in existing_tables:
        clickhouse.execute(f"RENAME TABLE {table_names['main']} TO {table_names['backup']}")
        logger.info(f"Table {table_names['backup']} updated with data from {table_names['main']}.")

    clickhouse.execute(f"RENAME TABLE {table_names['temp']} TO {table_names['main']}")

    logger.info(f"Export successful, table saved as {table_names['main']}.")
