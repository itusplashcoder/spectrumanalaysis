import unittest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime

from metrics.top_10_keywords.entrypoint import (
    top_10_keywords_by_country,
    top_10_keywords_by_category,
)

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


class TopKeywordsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]").appName("Test").getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_top_10_keywords_by_country(self):
        # Mock data
        data = [
            Row(
                id="1",
                category_id="1",
                news_id="n1",
                country_id="1",
                news_url="url1",
                news_text="The quick brown fox jumps over the lazy dog.",
                news_title="title1",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="2",
                category_id="1",
                news_id="n2",
                country_id="1",
                news_url="url2",
                news_text="The quick brown fox.",
                news_title="title2",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="3",
                category_id="2",
                news_id="n3",
                country_id="2",
                news_url="url3",
                news_text="Hello world! Hello universe.",
                news_title="title3",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="4",
                category_id="2",
                news_id="n4",
                country_id="2",
                news_url="url4",
                news_text="Hello world!",
                news_title="title4",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
        ]
        df = self.spark.createDataFrame(data, schema=FACT_NEWS_SCHEMA)
        df.createOrReplaceTempView("fact_news")

        top_10_keywords_by_country(self.spark)

        result_df = self.spark.table("trending_keywords_by_country")
        result_df.show()

        # Check if the table exists and has the correct columns
        self.assertTrue("country_id" in result_df.columns)
        self.assertTrue("word" in result_df.columns)
        self.assertTrue("frequency" in result_df.columns)

    def test_top_10_keywords_by_category(self):
        # Mock data
        data = [
            Row(
                id="1",
                category_id="1",
                news_id="n1",
                country_id="1",
                news_url="url1",
                news_text="The quick brown fox jumps over the lazy dog.",
                news_title="title1",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="2",
                category_id="1",
                news_id="n2",
                country_id="1",
                news_url="url2",
                news_text="The quick brown fox.",
                news_title="title2",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="3",
                category_id="2",
                news_id="n3",
                country_id="2",
                news_url="url3",
                news_text="Hello world! Hello universe.",
                news_title="title3",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
            Row(
                id="4",
                category_id="2",
                news_id="n4",
                country_id="2",
                news_url="url4",
                news_text="Hello world!",
                news_title="title4",
                created_at=datetime.now(),
                updated_at=datetime.now(),
            ),
        ]
        df = self.spark.createDataFrame(data, schema=FACT_NEWS_SCHEMA)
        df.createOrReplaceTempView("fact_news")

        top_10_keywords_by_category(self.spark)

        result_df = self.spark.table("trending_keywords_by_category")
        result_df.show()

        # Check if the table exists and has the correct columns
        self.assertTrue("category_id" in result_df.columns)
        self.assertTrue("word" in result_df.columns)
        self.assertTrue("frequency" in result_df.columns)


if __name__ == "__main__":
    unittest.main()
