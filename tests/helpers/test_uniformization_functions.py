import unittest
import datetime

from src.helpers.uniformization_functions import format_date
from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import SparkSession

import pyspark.sql.functions as F


class TestUniformizationFunctions(unittest.TestCase):
    def test_format_date(self):

        source_data = [
            ("2022-03-01",),
            ("12/03/2022",),
            ("1 january 2022",)
        ]

        source_df = self.spark.createDataFrame(source_data, ["date"])

        actual_df = source_df.withColumn(
            "clean_date",
            format_date(F.col("date"))
        )

        expected_schema = StructType([
            StructField('date', StringType()),
            StructField('clean_date', DateType())
        ])

        expected_data = [
            ("2022-03-01",datetime.date(2022, 3, 1)),
            ("12/03/2022",datetime.date(2022, 3, 12)),
            ("1 january 2022",datetime.date(2022, 1, 1)),
        ]
        expected_df = self.spark.createDataFrame(expected_data, expected_schema)

        assert_df_equality(actual_df, expected_df)


    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
