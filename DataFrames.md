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


A Spark DataFrame with the first 10 rows of the dataset (limit(10)), displayed using show().


df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
        .option("delimiter",":") \
    .load("dbfs:/databricks-datasets/learning-spark-v2/people/")

top10 = df.limit(10)

top10.show()
