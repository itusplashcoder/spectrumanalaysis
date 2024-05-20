import uuid
from schemas import *
from pyspark.sql.functions import col, lit, current_timestamp, expr


# Function to insert a list of new records into the fact_news table
def insert_into_fact_news(spark, records):
    new_records = []
    news_name, news_url, news_text, news_title, category_name, country_name = records[0]

    news_id = (
        spark.table("news").filter(f"name = '{news_name}'").select("id").collect()[0][0]
    )
    country_id = (
        spark.table("country")
        .filter(f"name = '{country_name}'")
        .select("id")
        .collect()[0][0]
    )

    for record in records:
        news_name, news_url, news_text, news_title, category_id, country_name = record

        # Create the new record
        new_record = (
            str(uuid.uuid4()),  # Generate UUID for id
            category_id,
            news_id,
            country_id,
            news_url,
            news_text,
            news_title,
            None,  # Placeholder for created_at
            None,  # Placeholder for updated_at
        )

        new_records.append(new_record)

    # Create DataFrame from the new records
    new_records_df = spark.createDataFrame(new_records, schema=FACT_NEWS_SCHEMA)

    # Add the current timestamp for created_at and updated_at
    new_records_df = new_records_df.withColumn(
        "created_at", current_timestamp()
    ).withColumn("updated_at", current_timestamp())

    return new_records_df
