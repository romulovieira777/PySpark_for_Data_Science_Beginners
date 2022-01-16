from pyspark.python.pyspark.shell import sqlContext
from pyspark.sql import SparkSession

train = sqlContext.load(source="com.databricks.spark.csv", path="folder_path", header=True, inferschema=True)

spark = SparkSession.builder.appName("PythonSparkCreateRDDexample").config("spark.some.config.option",
                                                                        "some-value").getOrCreate()
