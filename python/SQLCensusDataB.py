"""SQLCensusDataA.py"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import CensusData

spark = SparkSession.builder.appName("SQL Census Data A").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([ \
      StructField("age", IntegerType()), \
      StructField("workclass", StringType()), \
      StructField("fnlwgt", IntegerType()), \
      StructField("education", StringType()), \
      StructField("educationNum", IntegerType()), \
      StructField("maritalStatus", StringType()), \
      StructField("occupation", StringType()), \
      StructField("relationship", StringType()), \
      StructField("race", StringType()), \
      StructField("sex", StringType()), \
      StructField("capitalGain", IntegerType()), \
      StructField("capitalLoss", IntegerType()), \
      StructField("hoursPerWeek", IntegerType()), \
      StructField("nativeCountry", StringType()), \
      StructField("income", StringType()) \
])    
data = spark.read.schema(schema).option("header", True).csv("../data/adult.csv").cache()

n = data.count()
print("Fraction > 50K = " + str(data.filter(lambda cd: cd.incomeOver50).count() / n))
print("Average age = " + str(data.map(lambda cd: cd.age).reduce(lambda a, b: a + b) / n))
over50years = data.filter(lambda cd: cd.age >= 50)
print("Fraction > 50K in 50+ age group = " + str(over50years.filter(lambda cd: cd.incomeOver50).count() / over50years.count()))
married = data.filter(lambda cd: cd.maritalStatus == "Married-civ-spouse")
print("Fraction > 50K in married group = " + str(married.filter(lambda cd: cd.incomeOver50).count() / married.count()))
print("Quartile age = " + str(data.map(_.age).stat.approxQuantile("value", Array(0.25, 0.5, 0.75), 0.1)))
print("Fraction by race")
fracAgg = (FracOver50K).toColumn
raceCounts = data.groupByKey(lambda cd: cd.race).agg(fracAgg)
for row in raceCounts.collect():
    print(row)
print("Fraction work more than 40 hrs/week = " + data.filter(lambda cd: cd.hoursPerWeek > 40).count() / n)

spark.stop()
