# Databricks notebook source
'''Ariel Harris - Lab 6'''

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
import pyspark.sql.functions as f


spark = SparkSession \
    .builder \
    .appName('Python Spark SQL')\
    .getOrCreate()

sc = spark.sparkContext

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

masterDF = spark.read.format("csv").option('header', True).option('inferSchema', True).load("/FileStore/tables/Master.csv").select("playerID","nameFirst","nameLast")
allStarDF = spark.read.format("csv").option('header', True).option('inferSchema', True).load("/FileStore/tables/AllstarFull.csv").select("playerID","teamID")
teamsDF = spark.read.format("csv").option('header', True).option('inferSchema', True).load("/FileStore/tables/Teams.csv").select("teamID","name")

# COMMAND ----------

allStarTeamDF = allStarDF.join(teamsDF, 'teamID').join(masterDF, 'playerID').distinct().withColumnRenamed("name","teamName")
allStarTeamDF.show()

# COMMAND ----------

allStarTeamDF.write.format('parquet').mode('overwrite').partitionBy('teamName').save('/FileStore/tables/byTeamName')

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/byTeamName

# COMMAND ----------

allStarTeamParq = spark.read.format('parquet').load('/FileStore/tables/byTeamName')
rockies = allStarTeamParq.select('nameFirst','nameLast').filter(f.col('teamName') == 'Colorado Rockies')
print("Num. of Colorado Rockies all stars:", rockies.count())
rockies.show(rockies.count())
