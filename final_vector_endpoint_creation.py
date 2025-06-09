# Databricks notebook source
# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
 
vsc = VectorSearchClient()
help(VectorSearchClient)

# COMMAND ----------

#workspace.lab_dataset.final_airbnb_properties_txt

catalog_name = "workspace"
schema_name = "lab_dataset"
source_table_name = "final_airbnb_properties_txt"
source_table_fullname = f"{catalog_name}.{schema_name}.{source_table_name}"

# COMMAND ----------

#enable change data feed
spark.sql("ALTER TABLE workspace.lab_dataset.final_airbnb_properties_txt SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

vector_search_endpoint_name = "final-airbnb-vector-search-endpoint"
vsc.create_endpoint(
    name=vector_search_endpoint_name,
    endpoint_type="STANDARD"
)

# COMMAND ----------

endpoint = vsc.get_endpoint(
  name=vector_search_endpoint_name)
endpoint

# COMMAND ----------

vs_index = "airbnb_index"
vs_index_fullname = f"{catalog_name}.{schema_name}.{vs_index}"
 
embedding_model_endpoint = "databricks-bge-large-en"

# COMMAND ----------

index = vsc.create_delta_sync_index(
  endpoint_name=vector_search_endpoint_name,
  source_table_name=source_table_fullname,
  index_name=vs_index_fullname,
  pipeline_type='TRIGGERED',
  primary_key="id",
  embedding_source_column="paragraph_text",
  embedding_model_endpoint_name=embedding_model_endpoint
)
index.describe()

# COMMAND ----------

index = vsc.get_index(endpoint_name=vector_search_endpoint_name, index_name=vs_index_fullname)
 
index.describe()

# COMMAND ----------

import time
while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):
    print("Waiting for index to be ONLINE...")
    time.sleep(10)
print("Index is ONLINE")
index.describe()