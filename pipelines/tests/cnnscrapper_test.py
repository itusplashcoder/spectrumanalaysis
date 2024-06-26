import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
from job_units.CnnScrapper.entrypoint import *


class TestCnnNewsScrapper(unittest.TestCase):

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
        # self.scrapper = CnnNewsScrapper(self.spark)

    def test_generate_random_string(self):
        random_string = CnnNewsScrapper.generate_random_string(16)
        self.assertEqual(len(random_string), 16)
        self.assertTrue(all(c.isalnum() for c in random_string))

    def test_clean_text(self):
        cleaned_text = CnnNewsScrapper.clean_text("Hello, World! This is a Test.")
        self.assertEqual(cleaned_text, "hello world this is a test")

    # @patch('cnn_news_scrapper.CnnNewsScrapper.get_nav_links', return_value=['/test_subcategory'])
    # @patch('cnn_news_scrapper.CnnNewsScrapper.get_article_links', return_value=['/test_article'])
    # @patch('cnn_news_scrapper.CnnNewsScrapper.get_meta_and_title', return_value={'news_text': 'test text', 'news_title': 'test title'})
    # @patch('cnn_news_scrapper.CnnNewsScrapper.generate_random_string', return_value='random_string')
    # def test_launch(self, mock_generate_random_string, mock_get_meta_and_title, mock_get_article_links, mock_get_nav_links):
    #     news_list = self.scrapper.launch()
    #     expected_news_list = [
    #         ('cnn', 'https://edition.cnn.com/test_article', 'test text', 'test title', '', 'united states')
    #     ]
    #     self.assertEqual(news_list, expected_news_list)


if __name__ == "__main__":
    unittest.main()
