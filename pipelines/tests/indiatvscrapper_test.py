import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup
from job_units.IndiaTVNewsScrapper.entrypoint import *


class TestIndiaTVNewsScrapper(unittest.TestCase):

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

    def test_generate_random_string(self):
        random_string = IndiaTVNewsScrapper.generate_random_string(16)
        self.assertEqual(len(random_string), 16)
        self.assertTrue(all(c.isalnum() for c in random_string))

    def test_clean_text(self):
        cleaned_text = IndiaTVNewsScrapper.clean_text("Hello, World! This is a Test.")
        self.assertEqual(cleaned_text, "hello world this is a test")


if __name__ == "__main__":
    unittest.main()
