from pyspark import SparkContext, SparkConf
from pyspark.shell import sc

conf = SparkConf().setAppName("reduce").setMaster("local[*]")
inputIntegers = [1, 2, 3, 4, 5]
integerRdd = sc.parallelize(inputIntegers)
product = integerRdd.reduce(lambda x, y: x * y)
print("product is : {}".format(product))


conf = SparkConf().setAppName("primeNumbers").setMaster("local[*]")
lines = sc.textFile("caminho_do_arquivo")
numbers = lines.flatmap(lambda line: line.split("\t"))
validNumbers = numbers.filter(lambda number: number)
intNumbers = validNumbers.map(lambda number: int(number))
print("Sum is : {}".format_map(intNumbers.reduce(lambda x, y: x + y)))


conf = SparkConf().setAppName("Count").setMaster("local[*]")
inputWords = ["Spark", "Hadoop", "Spark", "Hive", "Pig", "Cassandra", " Hadoop"]
wordRdd = sc.parallelize(inputWords)
print("Count is : {}".format(wordRdd.count()))
worldCountByValue = wordRdd.countByValue()
print("CountByValue: ")
for word, count in worldCountByValue.items():
    print("{} : {}".format(word, count))


conf = SparkConf().setAppName("take").setMaster("local[*]")
inputWords = ["Spark", "Hadoop", "Spark", "Hive", "Pig", "Cassandra", " Hadoop"]
wordRdd = sc.parallelize(inputWords)
words = wordRdd.take(3)
for word in words:
    print(word)
