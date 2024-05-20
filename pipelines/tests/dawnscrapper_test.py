import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
from job_units.DawnNewsScrapper.entrypoint import *


class TestDawnNewsScrapper(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
        # cls.spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        pass
        # cls.spark.stop()

    def setUp(self):
        pass
        # self.scrapper = DawnNewsScrapper(self.spark)

    def test_generate_random_string(self):
        random_string = DawnNewsScrapper.generate_random_string(16)
        self.assertEqual(len(random_string), 16)
        self.assertTrue(all(c.isalnum() for c in random_string))

    def test_clean_text(self):
        cleaned_text = DawnNewsScrapper.clean_text("Hello, World! This is a Test.")
        self.assertEqual(cleaned_text, "hello world this is a test")

    # @patch('dawn_news_scrapper.DawnNewsScrapper.get_nav_links', return_value=['/test_subcategory'])
    # @patch('dawn_news_scrapper.DawnNewsScrapper.get_article_links', return_value=['/test_article'])
    # @patch('dawn_news_scrapper.DawnNewsScrapper.get_meta_and_title', return_value={'news_text': 'test text', 'news_title': 'test title'})
    # @patch('dawn_news_scrapper.DawnNewsScrapper.generate_random_string', return_value='random_string')
    # def test_launch(self, mock_generate_random_string, mock_get_meta_and_title, mock_get_article_links, mock_get_nav_links):
    #     news_list = self.scrapper.launch()
    #     expected_news_list = [
    #         ('dawn', 'https://edition.dawn.com/test_article', 'test text', 'test title', '', 'united states')
    #     ]
    #     self.assertEqual(news_list, expected_news_list)


if __name__ == "__main__":
    unittest.main()
