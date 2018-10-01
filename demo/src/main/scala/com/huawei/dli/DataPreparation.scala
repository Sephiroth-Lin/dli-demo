package com.huawei.dli

import org.apache.spark.sql.SparkSession

object DataPreparation {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println(s"Wrong number of arguments! Expected 2 but got ${args.length}")
      return
    }

    // your AK/SK to access OBS
    val AK = "******"
    val SK = "******"

    val prefix = "s3a://" + AK + ":" + SK + "@"
    val readPath = prefix + args(0)
    val writePath = prefix + args(1)

    val spark = SparkSession
      .builder
      .appName("demo")
      .getOrCreate()

    // read the raw data
    val data = spark.read.textFile(readPath)

    // you can do some transformations like filter, map, etc on data
    val cleanedData = data.filter { line =>
      // if the type of the first attribute is not Long, remove this record
      var isValid = true
      try {
        val attributes = line.split(",")
        attributes.head.toLong
      } catch {
        case _: Throwable =>
          isValid = false
      }
      isValid
    }

    // output the cleaned data we want
    cleanedData.write.text(writePath)

    spark.close()
  }
}