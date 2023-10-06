"""
This is an example library script file to show that internal calls can be made from the driver
and the ModelOp Spark Service will correctly include all necessary source files in the submit.
"""


def load(spark, input_asset_path):
    """
    The model code needs to load the input assets defined for the job in whichever way is best.
    In this example we are loading a known to be CSV file from the hdfs path provided.
    :param spark: the spark session
    :param input_asset_path: the hdfs path of the file to load
    :return: the dataframe with the loaded data from the file.
    """
    df = spark.read.format("csv").option("header", "true").load(input_asset_path)
    return df


def validate_input_format(input_asset):
    """
    If you need to validate the input provided, this will fail if validation requirements are not met
    :param input_asset: input asset definition
    """
    if ("fileFormat" in input_asset) and (input_asset["fileFormat"] == "JSON"):
        raise ValueError("Input file format is set as JSON but must be CSV")


def transform(spark, dataframe):
    """
    If any transformation operations are required we can pass the dataframe and the spark context
    to continue to transform it as needed.
    Note that additional job parameters can be passed via the Spark Config from ModelOp Center
    :param spark: the spark session
    :param dataframe: the dataframe to be transformed
    :return: the transformed dataframe
    """
    start_year = spark.sparkContext.getConf().get("spark.startDate")
    return dataframe.filter(dataframe.Year > start_year).orderBy(dataframe.Year)

