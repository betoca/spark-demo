from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, when, count, udf, to_json, spark_partition_id, collect_list, struct


def validate_input_format(input_asset):
    if ("fileFormat" in input_asset) and (input_asset["fileFormat"] == "JSON"):
        raise ValueError("Input file format is set as JSON but must be CSV")


def transform(spark, dataframe):
    startYear = spark.sparkContext.getConf().get("spark.startDate")
    return dataframe.filter(dataframe.Year > startYear).orderBy(dataframe.Year)


def load(spark, input_asset_path):
    df = spark.read.format("csv").option("header", "true").load(input_asset_path)
    return df
