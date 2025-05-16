from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark =  SparkSession.builder.appName("reading employee dataset").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") #to Suppress Warn Logs
csv_path ="/Users/shivakrishnameda/Documents/My_Sample_Project/DATA_SETS/Employee.csv"
df = spark.read.format("csv").option("header", True).load(csv_path)
df.show()
df.printSchema()
print("Initial number of partitions:", df.rdd.getNumPartitions())


df.distinct().show()

df.limit(3).show()

df.count() # not the last line in a cell

print("dropping duplicates-----")
df_dropcol = df.dropDuplicates(["Age"])
df_dropcol.show()

df.select("Age","Gender").show() #column selection

df1 = df.withColumn("ZipCode",F.expr("Null")) #column creation
df1.show()

df1.drop("ZipCode").show() #drop the selective column

df.withColumn("Nation",F.lit(""))\
.withColumn("ZipCode",F.lit("")).show() #column creation using lit and expr

df1.drop("ZipCode").show()

df.printSchema()

df_new = df.withColumn("PaymentTier",F.col("PaymentTier").cast("double")) #Here converted string to double type
df_new.show()

df_new =  df.withColumnRenamed("Gender","Sex") # Column Renamed
df_new.show()

df_age = df.where("Age<25") # using filter/Where
df_age.show(5)


df_JoiningYear = df.filter(df["JoiningYear"]<2013) #same using filter
df_JoiningYear.show()

df_when1 = df.withColumn("Status", F.when(F.col("age")>30, "Adult").otherwise("Minor")) # using when for Conditional logic
df_when1.show()

df_when2 = df.select("JoiningYear","City","PaymentTier","Age",F.when(F.col("Age")<30, "Minor").otherwise("Adult").alias("Status"))
#using When for select Columns
df_when2.show()


df_lit = df.withColumn("Review", F.lit("Good"))
df_lit.show()

df_alias = df_lit.select("*",F.col("Review").alias("Performace"))
df_alias.show()
df_rename = df_lit.withColumnRenamed("Review","Performance")
df_rename.show()

df_sort = df.sort(F.col("Age",).asc())
df_sort.show(10)

df_order = df.orderBy(F.col("Age").asc())
df_order.show(10)

print("showing Repartitions")
df_repart = df.repartition(5)
df_repart.show()


df.write.mode("overwrite").option("Header", True).partitionBy("City").csv("/Users/shivakrishnameda/Documents/DATA_SETS/Processed_by_CITY/")

df.write.mode("Append").option("Header", True).partitionBy("Education").csv("/Users/shivakrishnameda/Documents/DATA_SETS/Processed_by_Education/")