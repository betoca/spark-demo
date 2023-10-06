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
    output_dir = SPARK.sparkContext.getConf().get("spark.outputDir")
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
                      "max": list(df.select("Max").toPandas().to_dict('list')["Max"]),
                      "min": list(df.select("Min").toPandas().to_dict('list')["Min"]),
                    }, categories=list(df.select(df.Year).toPandas().to_dict('list')["Year"]),
                key=basename + "_bar_graph", title=basename, x_axis_label="Year", y_axis_label="Values", rotated=True))
            schema_field_list.append(mtr.bar_graph_schema_field(bar_chart_key=basename + "_bar_graph", bar_chart_col_names=['max', 'min']))

        if "Count" in df.columns:
            mtr_output.update(mtr.as_bar_chart_data({
                      "count": list(df.select("Count").toPandas().to_dict('list')["Count"]),
                    }, categories=list(df.select("Year").toPandas().to_dict('list')["Year"]),
                key=basename + "_count_x_year", title=basename, x_axis_label="Year", y_axis_label="Values", rotated=False))
            schema_field_list.append(mtr.bar_graph_schema_field(bar_chart_key=basename + "_count_x_year", bar_chart_col_names=['count']))
        else:  # IP Rollover
            mtr_output.update(mtr.as_line_chart_data({
                "Rollover Current Year <=5%": list(
                    df.select("Year", "Rollover Current Year <=5%").toPandas().to_dict('split')['data']),
                "Rollover Current Year <=10%": list(
                    df.select("Year", "Rollover Current Year <=10%").toPandas().to_dict('split')['data']),
                "Rollover Current Year <=15%": list(
                    df.select("Year", "Rollover Current Year <=15%").toPandas().to_dict('split')['data']),
                "Rollover Current Year <=25%": list(
                    df.select("Year", "Rollover Current Year <=25%").toPandas().to_dict('split')['data']),
                "Rollover Current Year >25%": list(
                    df.select("Year", "Rollover Current Year >25%").toPandas().to_dict('split')['data']),
            }, key=basename + "_line", title=basename, x_axis_label="Year", y_axis_label="Values"))
            schema_field_list.append(mtr.line_graph_schema_field(line_chart_key=basename + "_line",
                                                                 line_chart_col_names=['Rollover Current Year <=5%',
                                                                                       'Rollover Current Year <=10%',
                                                                                       'Rollover Current Year <=15%',
                                                                                       'Rollover Current Year <=25%',
                                                                                       'Rollover Current Year >25%']))

            ip = df.select("Rollover Current Year <=5%", "Rollover Current Year <=10%", "Rollover Current Year <=15%",
                           "Rollover Current Year <=25%", "Rollover Current Year >25%", "Year")
            # Get the list of years (from the rows/values)
            years_list = list(ip.select("Year").toPandas().to_dict('list')["Year"])
            # For each Row where year = each item of the years list, get the rollover values per category
            tmp_list_data = list(map(lambda year: {year: list(ip.filter("Year = " + year).drop("Year").toPandas().to_dict('split')['data'][0])}, years_list))
            # Merge every item from the temp list
            data = {k: v for d in tmp_list_data for k, v in d.items()}

            mtr_output.update(mtr.as_bar_chart_data(data, categories=list(ip.drop("Year").columns),
                                                    key=basename + "_vertical_bar", title=basename,
                                                    x_axis_label="Values", y_axis_label="Year", rotated=False))
            schema_field_list.append(
                mtr.bar_graph_schema_field(bar_chart_key=basename + "_vertical_bar",
                                           bar_chart_col_names=years_list))

        # Use coalesce() so that the output CSV is a single file for easy reading
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(output_dir) + "/" + str(basename))

    print(mtr_output)

    schema = StructType(schema_field_list)
    row = Row(**mtr_output)
    consolidated_df = SPARK.createDataFrame([row], schema)
    consolidated_df.coalesce(1).write.mode('overwrite').json(output_asset_path)

    SPARK.stop()


# modelop.metrics
def metrics(external_inputs: List, external_outputs: List, external_model_assets: List):
    score(external_inputs, external_outputs, external_model_assets)
