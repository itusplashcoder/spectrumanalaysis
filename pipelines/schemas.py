from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Define schema for the fact table
FACT_NEWS_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("category_id", StringType(), True),
        StructField("news_id", StringType(), True),
        StructField("country_id", StringType(), True),
        StructField("news_url", StringType(), True),
        StructField("news_text", StringType(), True),
        StructField("news_title", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)


NEWS_DIM_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

COUNTRY_DIM_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

CATEGORY_DIM_SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
