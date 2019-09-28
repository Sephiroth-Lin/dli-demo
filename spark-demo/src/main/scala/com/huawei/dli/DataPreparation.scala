package com.huawei.dli

import org.apache.spark.sql.SparkSession

object DataPreparation {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println(s"Wrong number of arguments! Expected 4 but got ${args.length}")
      return
    }

    // get parameters
    val ak = args(0)
    val sk = args(1)
    val readPath = args(2)
    val writePath = args(3)

    val spark = SparkSession
      .builder
      .appName("scala_spark_demo")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", ak)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", sk)

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

    spark.stop()
  }
}