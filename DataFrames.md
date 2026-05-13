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






==================================================================================================


from pyspark.sql.functions import col
from pyspark.sql.window import Window

data = [
    (1, "Alice", "Sales", 50000, "2021-03-15"),
    (2, "Bob", "IT", 70000, "2020-07-22"),
    (3, "Charlie", "Sales", 60000, "2019-11-01"),
    (4, "Diana", "HR", 45000, "2022-01-10"),
    (5, "Eve", "IT", 80000, "2018-05-30"),
    (6, "Frank", "HR", 47000, "2023-06-18"),
    (None, "Ghost", None, None, None),
]  # dirty row - keep from Day 1columns = ["id","name","department","salary","join_date"]df = spark.createDataFrame(data, columns)df.show/()

columns = ["id","Name","Departement","Salary","JoinDate"]

df = spark.createDataFrame(data,columns)

df.select("Name","Departement","Salary")\
.filter((col("Departement") == "IT") & (col("Salary")>60000))\
.show()




=============================================================================================



from pyspark.sql.window import Window
from pyspark.sql.functions import avg,col

#paritionBy
#avg
#filter

data = [
    (1, "Alice", "Sales", 50000, "2021-03-15"),
    (2, "Bob", "IT", 70000, "2020-07-22"),
    (3, "Charlie", "Sales", 60000, "2019-11-01"),
    (4, "Diana", "HR", 45000, "2022-01-10"),
    (5, "Eve", "IT", 80000, "2018-05-30"),
    (6, "Frank", "HR", 47000, "2023-06-18"),
    (None, "Ghost", None, None, None),]

columns = ["id","Name","Departement","Salary","JoinDate"]
df= spark.createDataFrame(data,columns)
x=Window.partitionBy("Departement")
s_s=df.withColumn("Top_Salary",avg("Salary").over(x)).filter(col("Salary")> col("Top_Salary"))

s_s.show()




=================================================================================================




from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Dataprofiling").getOrCreate()

data = [
    (101, 2500.50, 5),
    (102, 3200.75, 10),
    (103, 1800.00, 2),
    (104, None, 8)
]

# Define columns
columns = ["customer_id", "transaction_amount", "reward_points"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

profile_desc = df.describe()


profile_desc.show(truncate=False)
