import requests
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import random
import string
from tqdm import tqdm
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, expr
import uuid
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from time import sleep
from utils import insert_into_fact_news


class IndiaTVNewsScrapper:
    def __init__(self, spark_session):
        self.__spark_session__ = spark_session
        self.__news_url__ = "https://indiatvnews.com/"
        self.__categories__ = {
            "world": "world",
            "sports": "sports",
            "tech": "technology",
            "entertainment": "entertainment",
            "business": "business",
        }

        self.__categories_id__ = {
            "world": "",
            "sports": "",
            "tech": "",
            "entertainment": "",
            "business": "",
        }
        self.__populate_categories_id__()

        news_df = self.__spark_session__.table("news")
        india_news_df = news_df.filter(news_df.url.contains(self.__news_url__))
        name_df = india_news_df.select("name")

        # Collect the names as a list
        self.__news_name__ = name_df.rdd.map(lambda row: row[0]).collect()[0]
        self.__country_name__ = "india"

    @staticmethod
    def generate_random_string(length=16):
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def clean_text(text):
        text = text.replace("\xa0", " ")
        text = text.lower()
        text = re.sub(r"[^a-z0-9\s]", "", text)
        return text

    def __populate_categories_id__(self):
        # SQL query to fetch category names and their IDs
        category_query = """
    SELECT id, name
    FROM category
    """
        # Execute the query
        category_df = self.__spark_session__.sql(category_query)

        # Collect the results into a dictionary
        category_dict = {row["name"]: row["id"] for row in category_df.collect()}

        # Update self.__categories_id__ with the fetched IDs
        for category in self.__categories_id__.keys():
            if category in category_dict:
                self.__categories_id__[category] = category_dict[category]

    def get_news_url(self):
        return self.__news_url__

    def set_news_url(self, url):
        try:
            assert type(url) == str
            assert url[0:8] == "https://"
            assert url[-4:0] == ".com"
            self.__news_url__ = url
        except Exception as error:
            print(str(error))

    def get_categories(self):
        return self.__categories__

    def set_categories(self, categories_dict):
        try:
            assert type(categories_dict) == dict
            self.__categories__ = categories_dict
        except Exception as error:
            print(str(error))

    def get_nav_links(self, category):
        # Send a GET request to the URL

        links = []
        for i in range(10):
            links.append(
                self.__news_url__
                + self.__categories__[category]
                + "/"
                + str(i + 1)
                + "/"
            )

        return links

    def get_article_links(self, url):
        # Send a GET request to the URL
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the page
            soup = BeautifulSoup(response.content, "html.parser")
            # Find all <a> tags with the specified class
            article_links = soup.find_all("a")
            # Extract href attributes from the <a> tags
            links = [link.get("href") for link in article_links]
            return links
        else:
            print(
                f"Failed to retrieve the webpage. Status code: {response.status_code}"
            )
            return []

    def get_meta_and_title(self, url):
        # Send a GET request to the URL
        if "https://" in url:
            response = requests.get(url)
        else:
            response = requests.get(self.__news_url__ + url[1:])

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the content of the page
            soup = BeautifulSoup(response.content, "html.parser")
            final_article = [p.get_text() for p in soup.find_all("p")]
            # Extract the title
            title = soup.title.string if soup.title else "No title found"
            title = title.replace("indiatvnews.com", "")

            return {
                "news_text": IndiaTVNewsScrapper.clean_text("".join(final_article)),
                "news_title": IndiaTVNewsScrapper.clean_text(title),
            }

        else:
            print(
                f"Failed to retrieve the webpage. Status code: {response.status_code}"
            )
            return {
                "news_text": "",
                "news_title": "",
            }

    def launch(self):
        # Get the current date and time
        current_datetime = datetime.now()

        # Format the datetime as MM/DD/YYYY HH:MM:SS
        formatted_datetime = current_datetime.strftime("%m/%d/%Y %H:%M:%S")
        list_of_news_objects = []
        for category in tqdm(self.__categories__, desc="Categories"):
            subcategories = self.get_nav_links(category)
            for subcategory in tqdm(subcategories, desc="Subcategories"):
                article_links = self.get_article_links(subcategory)
                for article_link in tqdm(article_links, desc="Article Link"):
                    info_dict = self.get_meta_and_title(article_link)

                    if info_dict["news_text"] != "":
                        db_object = (
                            self.__news_name__,
                            self.__news_url__ + article_link[1:],
                            info_dict["news_text"],
                            info_dict["news_title"],
                            self.__categories_id__[category],
                            self.__country_name__,
                        )
                        list_of_news_objects.append(db_object)
        return list_of_news_objects


if __name__ == "__main__":
    spark = SparkSession.builder.appName("indiatvScrapper").getOrCreate()
    indiatv_list = IndiaTVNewsScrapper(spark).launch()
    indiatv_df = insert_into_fact_news(indiatv_list)
    indiatv_df = indiatv_df.dropDuplicates(["news_title", "news_url", "news_text"])
    indiatv_df.write.format("delta").mode("append").saveAsTable("fact_news")
