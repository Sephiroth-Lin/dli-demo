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
  if len(sys.argv) != 4:
    print "Wrong number of arguments! Expected 4 but got " + str(len(sys.argv))
    exit(-1)

  # get parameters
  ak = sys.argv[1]
  sk = sys.argv[2]
  readPath = sys.argv[3]
  writePath = sys.argv[4]

  spark = SparkSession.builder.appName("pyspark_demo").getOrCreate()
  spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", ak)
  spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", sk)

  # read the raw data
  data = spark.read.text(readPath).rdd.map(lambda r: r[0])

  # if the type of the first attribute is not Long, remove this record
  cleanedData = data.filter(headIsLong)

  # output the cleaned data we want
  cleanedData.saveAsTextFile(writePath)

  spark.stop()
