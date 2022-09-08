# Databricks notebook source
# MAGIC %run ../../bootstrap/bootstrap_base

# COMMAND ----------

import daipe as dp

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create_for_exploration()

# COMMAND ----------

entity = dp.fs.get_entity()

# COMMAND ----------

@dp.transformation(display=True)
def get_features_metadata(feature_store: dp.fs.FeatureStore, widgets: dp.Widgets):
    return feature_store.get_metadata(
        entity_name=entity.name,
        templates=widgets.get_value("templates"),
        categories=widgets.get_value("categories"),
        time_windows=widgets.get_value("time_windows"),
        include_tags=widgets.get_value("tags"),
    )
