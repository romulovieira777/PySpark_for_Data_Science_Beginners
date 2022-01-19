import mysql
from pyspark.shell import sqlContext, sc
from pyspark import SparkContext, SparkConf

dataframe_mysql = sqlContext.read.format("jdbc").options(url="jdbc:mysql//3306/demo", driver="com.mysql.jdbc.Driver",
                                                         dbtable="StockPrices", user="root", password="root123").load()
dataframe_mysql.show()

Symbols = dataframe_mysql.select("symbol")
Symbols.show()

dataframe_mysql.registerTempTable("Stock")
rersult = sqlContext.sql("select max(volumn) as max from Stock where symbol = 'A'")
rersult.show()

rersult = sqlContext.sql("select max(volumn) as max from Stock group by symbol")
rersult.show()

rersult = sqlContext.sql("select max(volumn) as max, symbol from Stock group by symbol")
rersult.show()

conf = SparkConf().setAppName("Word Count").setMaster("local[3]")
lines = sc.textFile("caminho do Arquivo")
words = lines.flatmap(lambda line: line.split(" "))
wordCounts = words.countByValue()
for word, count in wordCounts.items():
    print("{} : {}".format(word, count))
