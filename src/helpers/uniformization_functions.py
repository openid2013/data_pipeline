from pyspark.sql.functions import coalesce, to_date
from pyspark.sql import Column


def format_date(col: Column, formats = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "d MMMM yyyy"]) -> Column:
    """
    Spark sql function intended to uniformize date format on datasets
    :param col:
    :param formats:
    :return: Column
    """
    return coalesce(*[to_date(col, f) for f in formats])
