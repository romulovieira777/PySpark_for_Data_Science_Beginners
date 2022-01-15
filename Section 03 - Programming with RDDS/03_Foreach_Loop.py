from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("PythonSparkCreateRDDexample").config("spark.some.config.option",
                                                                        "some-value").getOrCreate()
word = sc.SparkContext.parallelize(
    ["Scala", "Java", "Hadoop", "Spark", "Akka", "Spark vs Hadoop", "Pyspark", "Pyspark and Spark"])


def f(x):
    print(x)


fore = word.foreach(f)
