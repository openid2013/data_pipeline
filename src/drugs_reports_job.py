import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, lit, col
from helpers.dataset_config import DatasetConfig
from helpers.dataset_loader import load_data
from helpers.job_config import JobConfig
from helpers.uniformization_functions import format_date

"""
This Job is intended to compute the all occurrences between drugs and publications.
"""

parser = argparse.ArgumentParser(description='Drugs data pipeline')

# parse Job Config
parser.add_argument('--job_config', required=True, help='Job config')

# parse data sources configs
parser.add_argument('--drugs_config', required=True, help='drugs data source config')
parser.add_argument('--pubmed_csv_config', required=True, help='pubmed csv data source config')
parser.add_argument('--pubmed_json_config', required=True, help='pubmed json data source config')
parser.add_argument('--clinical_trials_config', required=True, help='clinical trials data source config')


args = parser.parse_args()

job_config = JobConfig(args.job_config)

spark = SparkSession \
    .builder \
    .master(job_config.yarn_endpoint) \
    .getOrCreate()

# read data sources
drugs = load_data(spark, DatasetConfig(args.drugs_config))

pubmed = \
    load_data(spark, DatasetConfig(args.pubmed_csv_config)) \
        .union(
            load_data(spark, DatasetConfig(args.pubmed_json_config)))
clinical_trials = load_data(spark, DatasetConfig(args.clinical_trials_config))

# reconciliate drugs with pubmed data
drugs_pubmed_reconciliation = drugs \
    .join(pubmed,
          upper(pubmed.title).contains(upper(drugs.drug))) \
    .withColumn("publication_origin", lit("pubmed"))

# reconciliate drugs with clinical trials data
drugs_clinical_trials_reconciliation = drugs \
    .join(clinical_trials, upper(clinical_trials.scientific_title).contains(drugs.drug)) \
    .withColumn("publication_origin", lit("clinical_trials")) \
    .withColumnRenamed("scientific_title", "title")

# combine the both sub sets
drugs_publications = drugs_pubmed_reconciliation\
    .union(drugs_clinical_trials_reconciliation)

# uniformization step
# ideally should be done just after loading the raw data, but to prevent making the compute 2 times on 2 datasets,
# at this level, only one compute should simplify the code

drugs_publications_uniformized = drugs_publications\
    .withColumn("date", format_date(col("date")))

# output the result
if job_config.one_file_output:
    drugs_publications_uniformized\
        .toPandas()\
        .to_json(job_config.output_path,
                 orient='records',
                 indent=True,
                 date_format="iso",
                 force_ascii=False)
else:
    drugs_publications_uniformized.write.json(job_config.output_path)

print(f"""
- Input stats:
    - drugs: {drugs.count()} rows
    - pubmed: {pubmed.count()} rows
    - clinical_trials: {clinical_trials.count()} rows
    
- Output stats: {drugs_publications_uniformized.count()} rows
""")
