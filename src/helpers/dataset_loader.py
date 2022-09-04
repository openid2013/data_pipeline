from helpers.dataset_config import DatasetConfig
from pyspark.sql import SparkSession, DataFrame


def load_data(sparksession: SparkSession,
              data_config: DatasetConfig) -> DataFrame:
    """
    Dynamic data loader component, which bases on dataset config parameters
    :param sparksession:
    :param data_config:
    :return: DataFrame
    """

    reader = sparksession.read

    for key, value in data_config.data_type_options.items():
        reader = reader.option(key, value)

    return reader\
        .format(data_config.format) \
        .load(data_config.path) \
        .select(data_config.columns)
