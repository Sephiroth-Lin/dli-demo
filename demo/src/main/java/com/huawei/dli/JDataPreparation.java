package com.huawei.dli;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class JDataPreparation {
  public static void main(String args[]) {

    if (args.length != 2) {
      System.out.println("Wrong number of arguments! Expected 2 but got " + args.length);
      return;
    }

    // your AK/SK to access OBS
    String AK = "******";
    String SK = "******";

    String prefix = "s3a://" + AK + ":" + SK + "@";
    String readPath = prefix + args[0];
    String writePath = prefix + args[1];

    SparkSession spark = SparkSession
      .builder()
      .appName("demo")
      .getOrCreate();

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

    spark.close();
  }
}
