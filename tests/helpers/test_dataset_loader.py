import unittest
import sys


from pyspark.sql import SparkSession
from src.helpers.dataset_loader import load_data
from src.helpers.dataset_config import DatasetConfig


class TestDatasetLoader(unittest.TestCase):

    def test_init(self):
        config = DatasetConfig("./tests/data/clinical_trials.config")

        data = load_data(self.spark, config)

        assert(data.count() == 8)


    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
