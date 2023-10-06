from pyspark.sql.types import *

"""
Example library class to help process the formatting of the structure required
for a ModelOp Center - Model Test Result.
Do not use this as a generic library as it needs to work with the respective data set.
It should be a good example and guide to create the right structure.

Please refer to the following link, for additional information about the structure:
https://modelopdocs.atlassian.net/wiki/spaces/dv301/pages/1697154788/Model+Monitoring+Overview#Custom-Monitor-Output---Charts,-Graphs,-Tables
"""

# Methods in the section below will create the Schema definition for the chart as required by the structure linked above
# Please note that the data types can change depending on the input data being processed,
# so adjusting the method to receive a type definition or just changing the definition in this method might be in place.
# These methods mostly acts as a reminder of the structure and a way to standardize the data within the code.


def line_graph_schema_field(line_chart_key=None, line_chart_col_names=None):
    """
    Returns the Spark Schema for a line graph, use when generating a line chart structure dict,
    such as Spark is able to properly read the complex nested structure
    :param line_chart_key: the key of the object where the line chart is
    :param line_chart_col_names: the list of columns (fields) used in the data. These map to the series of the chart
    :return: The partial schema (StructField) that contains the structure for the line graph
    """
    partial_schema = StructField(
        line_chart_key,
        StructType([
            StructField("title", StringType(), False),
            StructField("x_axis_label", StringType(), False),
            StructField("y_axis_label", StringType(), False),
            StructField("data", StructType(
                list(map(lambda col: StructField(col, ArrayType(ArrayType(StringType()))), line_chart_col_names))
            ), False)
        ]), True
    )
    return partial_schema


def bar_graph_schema_field(bar_chart_key=None, bar_chart_col_names=None, categories_type=StringType()):
    """
    Returns the Spark Schema for a bar graph, use when generating a bar chart structure dict,
    such as Spark is able to properly read the complex nested structure
    :param bar_chart_key: the key of the object where the bar chart is
    :param bar_chart_col_names: the list of columns (fields) used in the data. These map to the series of the chart
    :param categories_type: data type used for the categories values
    :return: The partial schema (StructField) that contains the structure for the bar graph
    """
    partial_schema = StructField(
        bar_chart_key,
        StructType([
            StructField("title", StringType(), False),
            StructField("x_axis_label", StringType(), False),
            StructField("y_axis_label", StringType(), False),
            StructField("rotated", BooleanType(), True),
            StructField("data", StructType(
                list(map(lambda col: StructField(col, ArrayType(StringType())), bar_chart_col_names))
            ), False),
            StructField("categories", ArrayType(categories_type), False),
        ]), True
    )
    return partial_schema


def generic_table_schema_field(table_key="generic_table"):
    """
    Returns the Spark Schema for a generic table structure, this methods assumes all strings, but should be customized
    in case the data is required in other types.
    :param table_key: the key within the data dictionary where the table exists
    :return: the partial schema (StructField) that contains the structure for the tabular structure.
    """
    partial_schema = StructField(table_key, ArrayType(MapType(StringType(), StringType())))
    return partial_schema


# Methods in the section below will format the data for the chart as required by the structure linked above.
# Please note that the data should still be provided in the expected format.
# These methods mostly acts as a reminder of the structure and a way to standardize the data within the code.


def as_line_chart_data(data, key, title="", x_axis_label="", y_axis_label=""):
    """
    Returns a dictionary with the structure necessary for a line chart
    :param data: a dictionary in the format: {"series1": [[x1,y1], [x2,y2]] "series2": [[x1,y3], [x2,y3]]}
    :param key: of the returning dictionary entry
    :param title: of the bar chart
    :param x_axis_label: text to describe the values in the categories
    :param y_axis_label: text to describe the values in the data
    :return: The dictionary structure with bar chart data
    """
    return {
                key: {
                    "title": title,
                    "x_axis_label": x_axis_label,
                    "y_axis_label": y_axis_label,
                    "data": data
                }
            }


def as_bar_chart_data(data, categories, key, title="", x_axis_label="", y_axis_label="", rotated=False):
    """
    Returns a dictionary with the structure necessary for a bar chart
    :param data: a dictionary in the format: {"series1": ["a","b","c"], "series2": ["x","y","z"]}
    :param categories: a list of values matching the series in the data
    :param key: of the returning dictionary entry
    :param title: of the bar chart
    :param x_axis_label: text to describe the values in the categories
    :param y_axis_label: text to describe the values in the data
    :param rotated: true for horizontal, false for vertical (default)
    :return: The dictionary structure with bar chart data
    """
    return {
                key: {
                    "title": title,
                    "x_axis_label": x_axis_label,
                    "y_axis_label": y_axis_label,
                    "rotated": rotated,
                    "data": data,
                    "categories": categories
                }
            }


def as_tabular_data(dataframe, key=None):
    """
    Returns a Dictionary with the data in records format
    :param dataframe: A Spark DataFrame to return in tabular form
    :param key: The string to use as key to the dictionary entry
    :return: The Dictionary structure with the data
    """
    if key is not None:
        return {key: list(dataframe.toPandas().to_dict('records'))}
    else:
        return list(dataframe.toPandas().to_dict('records'))
