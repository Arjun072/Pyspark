from pyspark.sql.functions import spark_partition_id,col


data = spark.createDataFrame([
(1,"A"),
(2,"B"),
(3,"C"),
(4,"D"),
(5,"E"),
(6,"F"),
(7,"G"),
(8,"H")
], ["id","letter"])

data.select("id",col("letter").alias("L")).show()
