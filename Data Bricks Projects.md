##Source to Landing Zone




# loading data from source file to Delta table of landing Zone

df= spark.read.format("csv")\
    .option("header",True)\
    .option("inferschema",True)\
    .option("delimiter",":")\
    .load("dbfs:/databricks-datasets/learning-spark-v2/people/")



df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema",True) \
    .saveAsTable("Employee_Salary")


##From Landing Zone to Bronze Tables

#From Landing Zone to Bronze Tables

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CRITICAL_COLUMNS = ["id", "firstName", "salary"]
DEDUP_KEY_COLUMNS = ["id", "firstName"]

source_df = spark.table("employee_salary")

# ---------------------------------------------------------
# 1. Duplicates — your original logic, kept as is
# ---------------------------------------------------------
w = Window.partitionBy(*DEDUP_KEY_COLUMNS).orderBy("id")
ranked_df = source_df.withColumn("rn", F.row_number().over(w))

duplicate_df = ranked_df.filter("rn > 1").drop("rn") \
    .withColumn("rejection_reason", F.lit("DUPLICATE_RECORD"))

# ---------------------------------------------------------
# 2. Bad records — EXPLICIT null check, not a join
#    (a join on keys can never tell you "is this null",
#     only "does this key repeat")
# ---------------------------------------------------------
null_condition = F.lit(False)
for c in CRITICAL_COLUMNS:
    null_condition = null_condition | F.col(c).isNull()

bad_df = source_df.filter(null_condition) \
    .withColumn("rejection_reason", F.lit("NULL_IN_CRITICAL_FIELD"))

# ---------------------------------------------------------
# 3. Quarantine = bad UNION duplicates, appended (never overwritten)
# ---------------------------------------------------------
quarantine_df = bad_df.unionByName(duplicate_df)

quarantine_df = (
    quarantine_df
    .withColumnRenamed("rejection_reason", "error_reason")
    .withColumnRenamed("quarantine_timestamp", "quarantine_time")
    .withColumn("batch_id", F.lit(None).cast("string"))        # or your real batch id
    .withColumn("source_system", F.lit("employee_salary_landing"))  # or wherever this batch came from
)

quarantine_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("quarantine_employee")

# ---------------------------------------------------------
# 4. Clean = first occurrence (rn = 1) AND not null in critical cols
# ---------------------------------------------------------
clean_df = ranked_df.filter("rn = 1").filter(~null_condition).drop("rn")

clean_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("Bronze_employee_salary")


  ##Silver Layer

  #Null Handling,Unknown
#salary categorization
#writing in delta table
#

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType
)

from pyspark.sql.functions import col,when



x=spark.table("Bronze_employee_salary").select(
    "firstName",
    "salary",
    "lastName",
    when(col("salary").isNull(), "salary_missing")
    .when(col("salary") <= 50000, "low_salary")
    .when ((col("salary")>50000) & (col("salary") < 60000), "medium_salary")
    .when(col("salary") >= 60000, "high_salary")
    .otherwise("unknown")
    .alias("salary_category")                         
).orderBy(col("salary").desc())



x.write.format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable("people_salary")


spark.table("people_salary").show()
