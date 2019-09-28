package com.huawei.dli

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object KmeansDemo {
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

    val conf = new SparkConf().setAppName("KmeansExample")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", ak)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", sk)

    // Load and parse the data
    val data = sc.textFile(readPath)
    val header = data.first()
    val parsedData = data.filter(_ != header).map { s =>
      val attributes = s.split(",")
      // Use attributes 'latitude' and 'longitude' as the clustering features
      val array = Seq(attributes(16).toDouble, attributes(17).toDouble).toArray
      Vectors.dense(array)
    }

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save the model
    clusters.save(sc, writePath)
    sc.stop()
  }
}
