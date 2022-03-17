# Databricks notebook source
# MAGIC %md
# MAGIC ## Feature template notebook
# MAGIC This template notebook follows the [developing features with time windows tutorial](https://www.notion.so/datasentics/Developing-features-with-time-windows-d2dde276e7b94ded9b4925fd4a6a2f08), read that for more details

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Run bootstrap to able to use Daipe
# MAGIC Learn about what bootstrap does [here](https://www.notion.so/datasentics/Bootstrap-7afb00d3c5064a9986742ca80ad93cb0)

# COMMAND ----------

# MAGIC %run ../app/bootstrap

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Imports
# MAGIC Minimum imports, feel free to import other modules here

# COMMAND ----------

import daipe as dp
import featurestorebundle.time_windows as tw

from pyspark.sql import DataFrame #, functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Get entity and feature decorator
# MAGIC Minimum imports, feel free to import other modules here

# COMMAND ----------

entity = dp.fs.get_entity()
feature = dp.fs.feature_decorator_factory.create(entity)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Create control widgets
# MAGIC Learn about control widgets [here](https://www.notion.so/datasentics/Core-functionality-aafe501fa561496abd72c062532f16ec#e068537fc9f24f19a999ec1dc2952c7e)

# COMMAND ----------

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Load data
# MAGIC ##### TO DO:
# MAGIC Use [data loading functions](https://www.notion.so/datasentics/Data-loading-functions-e6f89bfd2c49473f8fde8bf25f6580bd) to load your data

# COMMAND ----------

@dp.transformation(display=False)
def load_data():
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Add timestamps
# MAGIC 
# MAGIC 
# MAGIC Feature store need a `timestamp` column to be able to register features. Learn more about why that is [here](https://www.notion.so/datasentics/Developing-features-with-time-windows-d2dde276e7b94ded9b4925fd4a6a2f08#2dc77d33bf534f598c3bf50113443b34)
# MAGIC 
# MAGIC ##### TO DO:
# MAGIC Change `your_date_column` to a column name which represents a date/timestamp of a transaction in your dataset

# COMMAND ----------

@dp.transformation(
    dp.fs.with_timestamps(
        load_data,
        entity,
        "your_date_column"
    ),
    display=False
)
def data_with_timestamps(df: DataFrame):
    return df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Make windowed
# MAGIC 
# MAGIC To be able to develop features using time windows, we need to create an instance of the [WindowedDataFrame](https://www.notion.so/datasentics/Time-windows-helper-classes-and-functions-c10623ae913d42e0a60b268b264b45a1#0b77fbf96f4d4e6e91e8dd3f965dc3f7)
# MAGIC 
# MAGIC ##### TO DO:
# MAGIC Change `your_date_column` to a column name which represents a date/timestamp of a transaction in your dataset

# COMMAND ----------

@dp.transformation(
    tw.make_windowed(
        data_with_timestamps,
        entity,
        "your_date_column",
    ),
    display=False
)
def data_with_time_windows(wdf: tw.WindowedDataFrame):
    return wdf.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Develop features
# MAGIC 
# MAGIC ##### TO DO:
# MAGIC Define your feature using the [declarative style](https://www.notion.so/datasentics/Developing-features-with-time-windows-d2dde276e7b94ded9b4925fd4a6a2f08#138466f927e047899fbacddd5d8c4df6), register it using the `feature` decorator to be written into Feature store.
# MAGIC 
# MAGIC ##### Documentation:
# MAGIC - [Feature](https://www.notion.so/datasentics/Core-functionality-aafe501fa561496abd72c062532f16ec#58db3786f9fc4f409116b0a1a17e9552)
# MAGIC - [fillna_with](https://www.notion.so/datasentics/Filling-NA-values-c9fdf67d7ad14e74a82e494721a90147)

# COMMAND ----------

@dp.transformation(data_with_time_windows, display=False)
#@feature(
#    dp.fs.Feature(
#        "example_feature_{time_window}",
#        "Example description in last {time_window}",
#        fillna_with=0
#    ),
#    category="your_category",
#)
def develop_features(wdf: tw.WindowedDataFrame):
    return wdf.time_windowed()
