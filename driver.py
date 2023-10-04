from __future__ import print_function

import sys
import os
from typing import List
from functools import reduce
import pandas as pd
import json

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, isnull, when, count, udf, to_json, spark_partition_id, collect_list, struct
import library


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
    outputDir = SPARK.sparkContext.getConf().get("spark.outputDir")
    output_asset_path = external_outputs[0]["fileUrl"]

    df_list = {}
    # Iterate through the inputs
    for idx in range(len(external_inputs)):
        # Simply echo the input (with filtered year)
        input_asset = external_inputs[idx]
        input_asset_path = input_asset["fileUrl"]
        basename = os.path.basename(input_asset_path)

        # Fail if assets are JSON
        library.validate_input_format(input_asset)

        df = library.load(SPARK, input_asset_path)
        df = library.transform(SPARK, df)

        df_list.update({basename + " data" : list(df.toPandas().to_dict('records'))})

        if "Max" in df.columns:
            bar_chart = {
                basename + "_bar_graph" : {
                    "title" : "Example Bar Chart",
                    "x_axis_label": "X Axis",
                    "y_axis_label": "Y Axis",
                    "rotated": False,
                    "data" : {
                      "max": list(df.select(df.Max).toPandas().to_dict('list')["Max"]),
                      "min": list(df.select(df.Min).toPandas().to_dict('list')["Min"]),
                    },
                    "categories": list(df.select(df.Year).toPandas().to_dict('list')["Year"])
                }
            }

            df_list.update(bar_chart)
        print(basename + " schema:")
        df.printSchema()

        # Use coalesce() so that the output CSV is a single file for easy reading
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(outputDir) + "/" + str(basename))

    # merged_df = reduce(lambda x, y: x.join(y, how = 'outer'), df_list)
    # merged_df.coalesce(1).write.mode("overwrite").json(output_asset_path)

    print(df_list)
    # generic = SPARK.read.json(SPARK.sparkContext.parallelize([df_list])).coalesce(1)
    # print("Merged schema:")
    # generic.printSchema()
    # generic.write.mode('overwrite').json(output_asset_path)

    row = Row(**df_list)
    df = spark.createDataFrame([row], mtr_format.include())
    df.createDataFrame([row]).coalesce(1).write.mode('overwrite').json(output_asset_path)

    # SPARK.createDataFrame([df_list]).coalesce(1).write.mode('overwrite').json(output_asset_path)

    # rdd_of_rows = rdd.map(lambda x: Row(**x))
    # df = sql.createDataFrame(rdd_of_rows)
    # df.printSchema()
    # df = df_from_rdd(rdd, prototype, sqlContext)

    # json_object = json.dumps(df_list)
    # # Writing to sample.json
    # with open("sample.json", "w") as outfile:
    #     outfile.write(json_object)

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs: List, external_outputs: List, external_model_assets: List):
    score(external_inputs, external_outputs, external_model_assets)
