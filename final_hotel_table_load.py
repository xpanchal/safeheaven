# Databricks notebook source
#Hotel listings sample dataset
booking_hotel_listings_df = spark.table("sample_datasets.bright_initiative.booking_hotel_listings")
display(booking_hotel_listings_df)

# COMMAND ----------

# DBTITLE 1,h
booking_hotel_listings_df = booking_hotel_listings_df.filter(booking_hotel_listings_df.country == "US")
display(booking_hotel_listings_df)

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, monotonically_increasing_id

# Convert each row into a paragraph of text by concatenating all columns with labels
columns = booking_hotel_listings_df.columns
hotel_text_df = booking_hotel_listings_df.select(
    monotonically_increasing_id().alias("id"),
    concat_ws(
        ". ",
        *[concat_ws(": ", col(c).cast("string").alias(c)) for c in columns]
    ).alias("paragraph_text")
)

display(hotel_text_df)

# COMMAND ----------

hotel_text_df.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("workspace.lab_dataset.booking_hotel_txt")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM workspace.lab_dataset.booking_hotel_txt