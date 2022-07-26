from pyspark import SparkConf
from pyspark.sql import *
from pyspark.sql import functions as F
from utility import *

spark = getSparkSession("sample-pyspark")

emp = [("John", 5000),
       ("Marry", 3000),
       ("Magnus", 6000),
       ("Eric", 2000)]

empSchema = ["name", "salary"]

empDF = spark.createDataFrame(data=emp, schema=empSchema)

maxSal = getMaxValue(empDF,"salary" )
print(maxSal)

