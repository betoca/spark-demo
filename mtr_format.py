from pyspark.sql import Row
from pyspark.sql.types import *

def infer_schema(rec):
    """infers dataframe schema for a record. Assumes every dict is a Struct, not a Map"""
    if isinstance(rec, dict):
        return StructType([StructField(key, infer_schema(value), True)
                              for key, value in sorted(rec.items())])
    elif isinstance(rec, list):
        if len(rec) == 0:
            raise ValueError("can't infer type of an empty list")
        elem_type = infer_schema(rec[0])
        for elem in rec:
            this_type = infer_schema(elem)
            if elem_type != this_type:
                raise ValueError("can't infer type of a list with inconsistent elem types")
        return ArrayType(elem_type)
    else:
        return _infer_type(rec)


def _rowify(x, prototype):
    """creates a Row object conforming to a schema as specified by a dict"""

    def _equivalent_types(x, y):
        if type(x) in [str, unicode] and type(y) in [str, unicode]:
            return True
        return isinstance(x, type(y)) or isinstance(y, type(x))

    if x is None:
        return None
    elif isinstance(prototype, dict):
        if type(x) != dict:
            raise ValueError("expected dict, got %s instead" % type(x))
        rowified_dict = {}
        for key, val in x.items():
            if key not in prototype:
                raise ValueError("got unexpected field %s" % key)
            rowified_dict[key] = _rowify(val, prototype[key])
            for key in prototype:
                if key not in x:
                    raise ValueError(
                        "expected %s field but didn't find it" % key)
        return Row(**rowified_dict)
    elif isinstance(prototype, list):
        if type(x) != list:
            raise ValueError("expected list, got %s instead" % type(x))
        return [_rowify(e, prototype[0]) for e in x]
    else:
        if not _equivalent_types(x, prototype):
            raise ValueError("expected %s, got %s instead" %
                             (type(prototype), type(x)))
        return x


def df_from_rdd(rdd, prototype, sql):
    """creates a dataframe out of an rdd of dicts, with schema inferred from a prototype record"""
    schema = infer_schema(prototype)
    row_rdd = rdd.map(lambda x: _rowify(x, prototype))
    return sql.createDataFrame(row_rdd, schema)


def bar_graph_schema_field(bar_chart_col_names = None, bar_chart_title = None, categories_type = IntegerType()):
    partial_schema = StructField(
        bar_chart_title,
        StructType([
            StructField("title", StringType(), False),
            StructField("x_axis_label", StringType(), False),
            StructField("y_axis_label", StringType(), False),
            StructField("rotated", StringType(), True),
            StructField("data", StructType([
                StructField("max", ArrayType(IntegerType())),
                StructField("min", ArrayType(IntegerType()))
            ]), False),
            StructField("categories", ArrayType(categories_type), False),
        ]),
            True
    )
    return partial_schema


def generic_table_schema_field(title = "generic_table"):
    partial_schema = StructField(title,ArrayType(MapType(StringType(),StringType())))
    return partial_schema
