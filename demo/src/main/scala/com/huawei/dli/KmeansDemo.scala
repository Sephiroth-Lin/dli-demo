package com.huawei.dli

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

object KmeansDemo {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println(s"Wrongnumber of arguments! Expected 2 but got ${args.length}")
      return
    }

    // your AK/SK to access OBS
    val AK = "******"
    val SK = "******"

    val prefix = "s3a://" + AK + ":" + SK + "@"
    val readPath = prefix + args(0)
    val writePath = prefix + args(1)

    val conf = new SparkConf().setAppName("KMeansExample")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = sc.textFile(readPath)
    val parsedData = data.map { s =>
      val attributes = s.split(",")
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

    // Save and load model
    clusters.save(sc, writePath)
    sc.stop()
  }
}
