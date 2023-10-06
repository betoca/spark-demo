from __future__ import print_function

import sys
import os
from typing import List

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
import transform_library as lib
import mtr_format as mtr

SPARK: SparkSession


# modelop.init
def init():
    global SPARK
    SPARK = SparkSession.builder.appName("Monitor Test").getOrCreate()
    conf = SPARK.sparkContext.getConf()

    print("spark.startDate = ", conf.get("spark.startDate"))
    print(str(conf.getAll()))
    print("Sys Arguments", sys.argv)


# modelop.score
def score(external_inputs: List, external_outputs: List, external_model_assets: List):
    outputDir = SPARK.sparkContext.getConf().get("spark.outputDir")
    output_asset_path = external_outputs[0]["fileUrl"]

    mtr_output = {}
    schema_field_list = []
    # Iterate through the inputs
    for idx in range(len(external_inputs)):
        # Simply echo the input (with filtered year)
        input_asset = external_inputs[idx]
        input_asset_path = input_asset["fileUrl"]
        basename = os.path.basename(input_asset_path)

        # Fail if assets are JSON
        lib.validate_input_format(input_asset)

        df = lib.load(SPARK, input_asset_path)
        df = lib.transform(SPARK, df)

        mtr_output.update(mtr.as_tabular_data(df, key=basename))
        schema_field_list.append(mtr.generic_table_schema_field(basename))

        if "Max" in df.columns:
            mtr_output.update(mtr.as_bar_chart_data({
                      "max": list(df.select(df.Max).toPandas().to_dict('list')["Max"]),
                      "min": list(df.select(df.Min).toPandas().to_dict('list')["Min"]),
                    }, categories=list(df.select(df.Year).toPandas().to_dict('list')["Year"]),
                key=basename + "_bar_graph", title=basename, x_axis_label="Year", y_axis_label="Values", rotated=True))
            schema_field_list.append(mtr.bar_graph_schema_field(bar_chart_title=basename + "_bar_graph", bar_chart_col_names=['max', 'min'], categories_type=IntegerType()))

        print(basename + " schema:")
        df.printSchema()

        # Use coalesce() so that the output CSV is a single file for easy reading
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(outputDir) + "/" + str(basename))

    print(mtr_output)

    schema = StructType(schema_field_list)
    row = Row(**mtr_output)
    df = SPARK.createDataFrame([row], schema)
    df.coalesce(1).write.mode('overwrite').json(output_asset_path)

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs: List, external_outputs: List, external_model_assets: List):
    score(external_inputs, external_outputs, external_model_assets)
