from __future__ import print_function

import sys
import os
from typing import List
from functools import reduce
import pandas as pd
import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnull, when, count, udf, to_json, spark_partition_id, collect_list, struct


# modelop.init
def init():
    print("Begin function...")

    global SPARK
    SPARK = SparkSession.builder.appName("Echo Test").getOrCreate()
    conf = SPARK.sparkContext.getConf()

    print("spark.startDate = ", conf.get("spark.startDate"))
    print(str(conf.getAll()))
    print("Sys Arguments" , sys.argv)


# modelop.score
def score(external_inputs: List, external_outputs: List, external_model_assets: List):
    startYear = SPARK.sparkContext.getConf().get("spark.startDate")
    outputDir = SPARK.sparkContext.getConf().get("spark.outputDir")
    output_asset_path = external_outputs[0]["fileUrl"]

    df_list = []
    # Iterate through the inputs
    for idx in range(len(external_inputs)):
        # Simply echo the input (with filtered year)
        input_asset = external_inputs[idx]
        input_asset_path = input_asset["fileUrl"]
        basename = os.path.basename(input_asset_path)

        # Fail if assets are JSON
        if ("fileFormat" in input_asset) and (input_asset["fileFormat"] == "JSON"):
            raise ValueError("Input file format is set as JSON but must be CSV")

        df = SPARK.read.format("csv").option("header", "true").load(input_asset_path)
        df = df.filter(df.Year > startYear)

        df_list.append({basename : list(df.toPandas().to_dict('records'))})

        # Use coalesce() so that the output CSV is a single file for easy reading
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(outputDir) + "/" + str(basename))

    # merged_df = reduce(lambda x, y: x.join(y, how = 'outer'), df_list)
    # merged_df.coalesce(1).write.mode("overwrite").json(output_asset_path)
    SPARK.read.json(SPARK.sparkContext.parallelize(df_list))\
    .coalesce(1)\
    .select(to_json(struct(*df.columns)).alias("json"))\
    .groupBy(spark_partition_id())\
    .agg(collect_list("json").alias("json_list"))\
    .select(col("json_list").cast("string"))\
    .write.text(output_asset_path)

    # SPARK.read.json(SPARK.sparkContext.parallelize(df_list)).coalesce(1).write.mode('overwrite').json(output_asset_path)

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs: List, external_outputs: List, external_model_assets: List):
    score(external_inputs, external_outputs, external_model_assets)
