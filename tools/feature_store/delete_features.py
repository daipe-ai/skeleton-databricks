# Databricks notebook source
# MAGIC %run ../../bootstrap/bootstrap_base

# COMMAND ----------

import daipe as dp

from typing import List
from logging import Logger
from featurestorebundle.feature.deleter.FeaturesDeleter import FeaturesDeleter

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create_for_delete()

# COMMAND ----------

entity = dp.fs.get_entity()

# COMMAND ----------

@dp.notebook_function()
def get_features_to_delete(feature_store: dp.fs.FeatureStore, widgets: dp.Widgets, logger: Logger) -> List[str]:
    metadata = feature_store.get_metadata(
        entity_name=entity.name,
        templates=widgets.get_value("templates"),
    )
    
    features = [row.feature for row in metadata.collect()]
    features_str = "\n".join(features)
    
    logger.warning(f"You are about to delete those features:\n{features_str}")
    
    return features

# COMMAND ----------

@dp.notebook_function(get_features_to_delete)
def delete_features(features_to_delete: List[str], features_deleter: FeaturesDeleter):
    features_deleter.delete(features_to_delete)
