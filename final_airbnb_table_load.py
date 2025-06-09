# Databricks notebook source
#Airbnb Sample Dataset
airbnb_properties_df = spark.table("sample_datasets.bright_initiative.airbnb_properties_information")
display(airbnb_properties_df)

# COMMAND ----------

airbnb_us_properties_df = airbnb_properties_df.filter(airbnb_properties_df.location.contains("United States"))
display(airbnb_us_properties_df)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, monotonically_increasing_id

# Convert each row into a paragraph of text by concatenating all columns with labels
columns = airbnb_us_properties_df.columns
airbnb_text_df = airbnb_us_properties_df.select(
    monotonically_increasing_id().alias("id"),
    concat_ws(
        ". ",
        *[concat_ws(": ", col(c).cast("string").alias(c)) for c in columns]
    ).alias("paragraph_text")
)

display(airbnb_text_df)

# COMMAND ----------

airbnb_text_df.write.mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.lab_dataset.final_airbnb_properties_txt")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.lab_dataset.final_airbnb_properties_txt