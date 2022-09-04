from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from helpers.job_config import JobConfig
import argparse

"""
This Job is intented to compute the Journal with the most drugs analysis
"""

parser = argparse.ArgumentParser(description='Journal with the most drugs analysis')
parser.add_argument('--job_config', required=True, help='drugs data source config')

# using the config job used to output the file that this job is using as input
parser.add_argument('--drugs_reports_job_config', required=True, help='drugs data source config')

# parser
args = parser.parse_args()

job_config = JobConfig(args.job_config)
drugs_reports_job_config = JobConfig(args.drugs_reports_job_config)

spark = SparkSession\
    .builder\
    .master(job_config.yarn_endpoint)\
    .getOrCreate()

# read input
reader = spark.read

if drugs_reports_job_config.one_file_output:
    reader = reader.option("multiLine", "True")

result = reader \
    .json(drugs_reports_job_config.output_path) \
    .select('atccode', 'journal') \
    .distinct() \
    .groupBy('journal') \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(1)

result\
    .toPandas()\
    .to_json(job_config.output_path, orient='records', indent=True)
