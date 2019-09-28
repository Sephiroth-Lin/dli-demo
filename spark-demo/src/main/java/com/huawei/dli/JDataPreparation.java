package com.huawei.dli;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class JDataPreparation {
  public static void main(String[] args) {

    if (args.length != 4) {
      System.out.println("Wrong number of arguments! Expected 4 but got " + args.length);
      return;
    }

    // get parameters
    String ak = args[0];
    String sk = args[1];
    String readPath = args[2];
    String writePath = args[3];

    SparkSession spark = SparkSession
      .builder()
      .appName("java_spark_demo")
      .getOrCreate();
    spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", ak);
    spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", sk);

    // read the raw data
    Dataset<String> data = spark.read().textFile(readPath);

    // you can do some transformations like filter, map, etc on data
    Dataset<String> cleanedData = data.filter((String line) -> {
      boolean isValid = true;
      String[] attributes = line.split(",");
      try {
        Integer.parseInt(attributes[0]);
      } catch (NumberFormatException e) {
        isValid = false;
      }
      return isValid;
    });

    // output the cleaned data we want
    cleanedData.write().text(writePath);

    spark.stop();
  }
}
