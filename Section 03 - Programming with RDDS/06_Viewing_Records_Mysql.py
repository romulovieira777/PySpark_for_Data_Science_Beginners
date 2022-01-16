from turtle import pd

from pyspark.shell import sc, sqlContext
from pyspark.sql import Row

import mysql

dataframe_mysql = sqlContext.read.format("jdbc").options(url="jdbc:mysql//3306/demo01", driver="com.mysql.jdbc.river",
                                                         dbtable="demo_table", user="root", password="root123").load()

dataframe_mysql.show()
dataframe_mysql.printschema()

my_list = [['a', 1, 2], ['b', 2, 3], ['c', 3, 4]]
col_name = ['A', 'B', 'C']
pd.DataFrame(my_list, columns=col_name)

l = [("Ankit", 25), ("Jalfaizy", 22), ("Saurabh", 20), ("Bala", 26)]
rdd = sc.parallelize(l)
people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
schemaPeople = sqlContext.createDataFrame(people)
type(schemaPeople)
