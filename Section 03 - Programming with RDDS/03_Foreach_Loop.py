from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("PythonSparkCreateRDDexample").config("spark.some.config.option",
                                                                        "some-value").getOrCreate()
words = sc.SparkContext.parallelize(
    ["Scala", "Java", "Hadoop", "Spark", "Akka", "Spark vs Hadoop", "Pyspark", "Pyspark and Spark"])


def f(x):
    print(x)


fore = words.foreach(f)
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Filtered RDD -> %s" % filtered)

words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print("Key value pair -> %s" % mapping)
