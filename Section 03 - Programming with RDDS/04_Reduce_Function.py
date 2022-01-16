from pyspark import SparkContext
from operator import add

from pyspark.sql import SparkSession

sc = SparkSession.builder.appName("PythonSparkCreateRDDexample").config("spark.some.config.option",
                                                                        "some-value").getOrCreate()
nums = sc.sparkContext.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print("Adding all the elements -> %i" % adding)


x = sc.sparkContext.parallelize([("spark", 1), ("Hadoop", 4)])
y = sc.sparkContext.parallelize([("spark", 2), ("Hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print("Join RDD -> %s" % final)


words = sc.SparkContext.parallelize(
    ["Scala", "Java", "Hadoop", "Spark", "Akka", "Spark vs Hadoop", "Pyspark", "Pyspark and Spark"])
words.cache()
caching = words.persist().is_cached
print("Words got cached -> %s" % caching)
