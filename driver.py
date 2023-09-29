from __future__ import print_function

import sys
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, when, count, udf


# modelop.init
def init():
    print("Begin function...", flush=True)

    global SPARK
    SPARK = SparkSession.builder.appName("Echo Test").getOrCreate()
    print("Spark variable:", SPARK, flush=True)

    conf = SPARK.sparkContext.getConf()
    print("sparkTmpJobWorkingFolder = ", conf.get("sparkTmpJobWorkingFolder"))
    print("startDate = ", conf.get("startDate"))
    print(str(conf.getAll()))


# modelop.score
def score(external_inputs: List, external_outputs: List, external_model_assets: List):
    # Grab single input asset and single output asset file paths
    input_asset_path, output_asset_path = parse_assets(
        external_inputs, external_outputs
    )
    input_df = SPARK.read.format("csv").option("header", "true").load(input_asset_path)

    # Use coalesce() so that the output CSV is a single file for easy reading
    input_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_asset_path)

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs: List, external_outputs: List, external_model_assets: List):
    score(external_inputs, external_outputs, external_model_assets)


def parse_assets(external_inputs: List, external_outputs: List):
    """Returns a tuple (input asset hdfs path, output asset hdfs path)"""

    # There's only one key-value pair in each dict, so
    # grab the first value from both
    input_asset = external_inputs[0]
    output_asset = external_outputs[0]

    # Fail if assets are JSON
    if ("fileFormat" in input_asset) and (input_asset["fileFormat"] == "JSON"):
        raise ValueError("Input file format is set as JSON but must be CSV")

    # Return paths from file URLs
    input_asset_path = input_asset["fileUrl"]
    output_asset_path = output_asset["fileUrl"]

    return (input_asset_path, output_asset_path)
