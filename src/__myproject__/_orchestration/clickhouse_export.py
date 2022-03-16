# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

from pyspark.sql import utils, SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.dbutils import DBUtils
from clickhouse_driver import Client as ClickhouseClient
from logging import Logger
import daipe as dp

# COMMAND ----------

entity = dp.fs.get_entity()

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config_url", "", "Config url")

# COMMAND ----------

@dp.notebook_function(dp.get_widget_value("config_url"))
def load_config(config_url: str, logger: Logger, dbutils: DBUtils, spark: SparkSession):
    try:
        config = spark.read.json(config_url).collect()[0].asDict()
    except utils.IllegalArgumentException:
        logger.error("Config path not set.")
        dbutils.notebook.exit(0)
        return None
    except utils.AnalysisException:
        logger.error(f"Couldn't find config at {config_url}.")
        dbutils.notebook.exit(0)
        return None
    else:
        logger.info(f"Loaded config from {config_url}")
        return config

# COMMAND ----------

current_env = load_config.result['env']
temp_table_name = f"featurestore_{current_env}_temp"
main_table_name = f"featurestore_{current_env}"
backup_table_name = f"featurestore_{current_env}_backup"

# COMMAND ----------

@dp.notebook_function("%odapfeatures.clickhouse.host%")
def clickhouse_connection(clickhouse_host: str, dbutils: DBUtils):
    return ClickhouseClient(
        clickhouse_host,
        user=dbutils.secrets.get(scope="unit-kv", key="clickhouse-user"),
        password=dbutils.secrets.get(scope="unit-kv", key="clickhouse-pass")
    )

# COMMAND ----------

@dp.notebook_function(clickhouse_connection)
def check_ongoing_export(clickhouse: ClickhouseClient, dbutils: DBUtils, logger: Logger):
    existing_tables = clickhouse.execute("SHOW TABLES")
    if (temp_table_name,) in existing_tables:
        logger.error(f"Clickhouse export is already ongoing in {current_env} environment.")
        dbutils.notebook.exit(0)

# COMMAND ----------

@dp.transformation(load_config, display=False)
def features_to_export(config: dict, feature_store: dp.fs.FeatureStore):
    return feature_store.get_latest(entity.name, features=config["params"].export_columns)

# COMMAND ----------

@dp.transformation(features_to_export, display=False)
def features_to_export_with_conversions(df: DataFrame):
    select_expr = [
        f.col(c).cast("float") if ("double" in t or "decimal" in t) else f.col(c) for c, t in df.dtypes
    ]
    return df.select(*select_expr)

# COMMAND ----------

@dp.transformation(features_to_export_with_conversions, "%odapfeatures.clickhouse.host%", "%odapfeatures.clickhouse.port%")
def write_features(df: DataFrame, clickhouse_host: str, clickhouse_port: int, dbutils: DBUtils, logger: Logger):
    logger.info(f"Writing features to ClickHouse database at {clickhouse_host}:{clickhouse_port}")
    (df.write
        .format("jdbc")
        .option("createTableOptions", "ENGINE = SummingMergeTree() ORDER BY client_id")
        .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
        .option("url", f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}")
        .option("dbtable", temp_table_name)
        .option("user", dbutils.secrets.get(scope="unit-kv", key="clickhouse-user"))
        .option("password", dbutils.secrets.get(scope="unit-kv", key="clickhouse-pass"))
        .mode('Overwrite')
        .save())

# COMMAND ----------

@dp.notebook_function(clickhouse_connection)
def rename_tables(clickhouse: ClickhouseClient, logger: Logger):
    existing_tables = clickhouse.execute("SHOW TABLES")
    clickhouse.execute(f"DROP TABLE IF EXISTS {backup_table_name}")

    if (main_table_name,) in existing_tables:
        clickhouse.execute(f"RENAME TABLE {main_table_name} TO {backup_table_name}")
        logger.info(f"Table {backup_table_name} updated with data from {main_table_name}.")

    clickhouse.execute(f"RENAME TABLE {temp_table_name} TO {main_table_name}")
    logger.info(f"Export successful, table saved as {main_table_name}.")
