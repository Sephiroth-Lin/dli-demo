import sys

from pyspark.sql import SparkSession

# check if the type of the first attribute is not Long
def headIsLong(x):
  arr = x.split(',')
  isValid = True
  try:
    longV = int(arr[0])
  except ValueError:
    isValid = False
  return isValid

if __name__ == "__main__":
  if len(sys.argv) != 2:
    print "Wrong number of arguments! Expected 2 but got " + str(len(sys.argv))
    exit(-1)

  # your AK/SK to access OBS
  ak = "******"
  sk = "******"

  prefix = "s3a://" + ak + ":" + sk + "@"
  readPath = prefix + sys.argv[1]
  writePath = prefix + sys.argv[2]

  spark = SparkSession.builder.appName("demo").getOrCreate()

  # read the raw data
  data = spark.read.text(readPath).rdd.map(lambda r: r[0])

  # if the type of the first attribute is not Long, remove this record
  cleanedData = data.filter(headIsLong)

  # output the cleaned data we want
  cleanedData.saveAsTextFile(writePath)

  spark.stop()
