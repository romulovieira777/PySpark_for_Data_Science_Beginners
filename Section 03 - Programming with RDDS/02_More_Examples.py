from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark create RDD example").config("spark.some.config.option",
                                                                               "some-value").getOrCreate()
mydata = spark.sparkContext.parallelize([(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)])
mydata.collect()

df = spark.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load("C:\Hadoop\bin")
df.show()
df.printSchema()
df.show()

sc = SparkSession.builder.appName("Python Spark create RDD example").config("spark.some.config.option",
                                                                            "some-value").getOrCreate()
word = sc.SparkContext.parallelize(["Scala", "Java", "Hadoop", "Spark", "Akka", "Spark vs Hadoop", "Pyspark", "Pyspark and Spark"])
counts = word.count()
print("Number of elements in RDD -> %i" % counts)
