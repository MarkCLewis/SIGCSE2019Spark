"""SQLCensusDataA.py"""
from pyspark.sql import SparkSession
import CensusData

class FracOver50K:
  def bufferEncoder():
      return Encoders.product
  def finish(reduction):
      reduction[0] / reduction[1]
  def merge(b1, b2):
    (b1[0] + b2[0], b1[1] + b2[1])
  def outputEncoder():
      Encoders.scalaDouble
  def reduce(b, a):
      (b[0] + (1 if (a.incomeOver50) else 0), b[1] + 1)
  def zero(): 
      (Int, Int) = (0, 0)



spark = SparkSession.builder.appName("SQL Census Data A").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

csvFile = spark.read.text("../data/adult.csv")
firstLine = csvFile.first()
print(firstLine["value"])
data = csvFile.filter("line != "+firstLine["value"]).map(CensusData.parseLine).cache()

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
