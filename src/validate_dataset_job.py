from enum import Enum
from pyspark.sql import SparkSession
import argparse
from helpers.dataset_config import DatasetConfig
from helpers.dataset_loader import load_data

""" 
This Job is intended to validate some technical aspects, such as the schema of the input file,
whether the columns are present as expected, and also to check if the file format is not corrupted.
We can imagine adding some business rules in the future, to validate the quality of data,
and gather further metrics on it, (nullable values, data types assertion ..)
"""

# supported data formats for validation process
class SupportedDataFormat(Enum):
    CSV = "csv"
    JSON = "json"


parser = argparse.ArgumentParser(description='Data source validation')
parser.add_argument('--data_source_config', required=True, help='data source config')

args = parser.parse_args()

data_source_config = DatasetConfig(args.data_source_config)

try:
    data_format = SupportedDataFormat[data_source_config.format.upper()]
except KeyError:
    print(f"The format type {data_source_config.format} is not supported yet, "
          f"here the supported data formats {[format_type.name for format_type in SupportedDataFormat]}")
    raise

spark = SparkSession \
    .builder \
    .getOrCreate()

try:
    data_input = \
        load_data(spark, data_source_config)

    print(f"""
        Data input(
        - data format type: {data_source_config.format}
        - path: {data_source_config.path}
        - columns: {data_source_config.columns}
        - number of records: {data_input.count()}
        ):is valid to be processed. 
    """)
except Exception:
    print("Data is corrupted")
    raise
