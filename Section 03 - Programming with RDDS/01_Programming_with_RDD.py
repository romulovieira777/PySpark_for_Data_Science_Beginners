from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark create RDD example").config("spark.some.config.option",
                                                                               "some-value").getOrCreate()
df = spark.sparkContext.parallelize([(1, 2, 3, 'a b c'), (4, 5, 6, 'd e f'), (7, 8, 9, 'g h i')]).toDF(
    ['col1', 'col2', 'col3', 'col4'])
df.show()

spark = SparkSession.builder.appName("Python Spark create RDD example").config("spark.some.config.option",
                                                                               "some-value").getOrCreate()
Employee = spark.createDataFrame(
    [('1', 'Joe', '700'), ('2', 'Henry', '80000', '2'), ('3', 'Sam', '60000', '2'), ('4', 'Max', '90000', '1')],
    ['Id', 'Name', 'Salary', 'DepartmentId'])
df.show()

Employee.show()
