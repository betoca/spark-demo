from pyspark.sql.types import *


def line_graph_schema_field(line_chart_title=None, line_chart_col_names=None):
    partial_schema = StructField(
        line_chart_title,
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


def bar_graph_schema_field(bar_chart_title=None, bar_chart_col_names=None, categories_type=StringType()):
    partial_schema = StructField(
        bar_chart_title,
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


def generic_table_schema_field(title="generic_table"):
    partial_schema = StructField(title, ArrayType(MapType(StringType(), StringType())))
    return partial_schema


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

