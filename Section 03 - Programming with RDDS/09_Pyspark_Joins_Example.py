from pyspark.shell import spark

valuesA = [("Pirate", 1), ("Monkey", 2), ("Ninga", 3), ("Spaghetti", 4)]
TableA = spark.createDataFrame(valuesA, ['name', 'id'])
valuesB = [("Rutabaga", 1), ("Pirate", 2), ("Ninja", 3), ("Darth Vader", 4)]
TableB = spark.createDataFrame(valuesB, ["name", "id"])
TableA.show()
TableB.show()
ta = TableA.alias("ta")
tb = TableB.alias("tb")

inner_join = ta.join(tb, ta.name == tb.name)
inner_join.show()

left_join = ta.join(tb, ta.name == tb.name, how="left")
left_join.show()

right_join = ta.join(tb, ta.name == tb.name, how="right")
right_join.show()

full_join = ta.join(tb, ta.name == tb.name, how="full")
full_join.show()

left_join = ta.join(tb, ta.name == tb.name, how="leftsemi")
left_join.show()

ta.crossJoin(tb)
ta.crossJoin(tb).show()
