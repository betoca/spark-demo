from pyspark.sql import Row
from pyspark.sql.types import *

def bar_graph_schema_field(bar_chart_col_names = None, bar_chart_title = None, bar_chart_data = [] , categories_type = IntegerType()):
    partial_schema = StructField(
        bar_chart_title,
        StructType([
            StructField("title", StringType(), False),
            StructField("x_axis_label", StringType(), False),
            StructField("y_axis_label", StringType(), False),
            StructField("rotated", BooleanType(), True),
            StructField("data", StructType([
                StructField("max", ArrayType(StringType())),
                StructField("min", ArrayType(StringType()))
            ]), False),
            StructField("categories", ArrayType(categories_type), False),
        ]),
            True
    )
    return partial_schema


def generic_table_schema_field(title = "generic_table"):
    partial_schema = StructField(title,ArrayType(MapType(StringType(),StringType())))
    return partial_schema
