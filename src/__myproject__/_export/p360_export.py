# Databricks notebook source
# MAGIC %run ../app/bootstrap

# COMMAND ----------

import daipe as dp
from p360_export.P360ExportRunner import P360ExportRunner
from pyspark.sql import DataFrame

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets: dp.Widgets):
    widgets.add_text("config_url", "", "Config url")

# COMMAND ----------

@dp.notebook_function()
def get_config_id(widgets: dp.Widgets):
    config_url = widgets.get_value("config_url")
    return config_url.split('/')[-1].replace('.json', '')

# COMMAND ----------

@dp.transformation(get_config_id, "@p360_export.config.config_getter")
def get_background_data(config_id: str, config_getter, feature_store: dp.fs.FeatureStore) -> DataFrame:
    config = config_getter.get(config_id=config_id)
    definition_persona = config.get("personas")[0].get("definition_persona")
    params = config.get("params")

    definition_attributes = [definition.get("attributes") for definition in definition_persona]
    attributes = {attribute["id"] for attributes in definition_attributes for attribute in attributes}
    attributes.update(set(params["export_columns"]))
    attributes.update(set(params["mapping"].values()))

    return feature_store.get_latest_attributes('client', attributes=list(attributes))

# COMMAND ----------

@dp.notebook_function(get_background_data, get_config_id)
def export(df_background: DataFrame, config_id: str, p360_export_runner: P360ExportRunner):
    p360_export_runner.config_getter.get(config_id=config_id)
    p360_export_runner.export(df_background=df_background, config_id=config_id)
