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


from abc import ABC, abstractmethod
import requests
from bs4 import BeautifulSoup

class NewsScrapper(ABC):
    def __init__(self, spark_session, news_url):
        self._spark_session = spark_session
        self._news_url = news_url
        self._categories = {}
        self._categories_id = {}
        self.populate_categories_id()

    @abstractmethod
    def populate_categories_id(self):
        """
        Populate the _categories_id dictionary with category names and their corresponding IDs from a data source.
        """
        pass

    @staticmethod
    def generate_random_string(length=16):
        import string
        import random
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def clean_text(text):
        import re
        text = text.replace("\xa0", " ")
        text = text.lower()
        text = re.sub(r"[^a-z0-9\s]", "", text)
        return text

    @abstractmethod
    def get_nav_links(self, category):
        """
        Retrieve navigation links for a given category from the news website.
        """
        pass

    @abstractmethod
    def get_article_links(self, url, html_class):
        """
        Fetch article links from a given URL, filtering by the specified HTML class.
        """
        pass

    @abstractmethod
    def get_meta_and_title(self, url):
        """
        Retrieve the meta information and title for a given article URL.
        """
        pass

    def launch(self):
        """
        The main method to launch the scraping process, which should be implemented based on specific scraper requirements.
        """
        pass
