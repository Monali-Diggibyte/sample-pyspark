from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def getSparkSession( appName):
    spark_conf = SparkConf()

    spark = SparkSession \
        .builder \
        .appName(appName) \
        .master("local[2]") \
        .getOrCreate()

    return spark


def getMaxValue(df, colname):
    result  = df.select(F.max(colname).alias("max")).collect()[0].asDict()
    return result["max"]
