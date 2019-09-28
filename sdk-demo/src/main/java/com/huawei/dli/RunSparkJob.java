package com.huawei.dli;

import java.io.File;
import java.util.UUID;

import com.huawei.dli.sdk.*;
import com.huawei.dli.sdk.common.SparkJobInfo;
import com.huawei.dli.sdk.common.SparkJobStatus;
import com.huawei.dli.sdk.exception.DLIException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunSparkJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunSparkJob.class);
  private static final Config CONF = new Config();

  public static void main(String[] args) {
    String bucketName = CONF.getValue("bucketName");
    String jarName = CONF.getValue("jarName", "dli-spark-demo-1.0.0.jar");
    String clusterName = CONF.getValue("clusterName", "4cu_sdk");

    LOGGER.info("Put resources to OBS");
    boolean success = Utils.putFileToOBS(bucketName, jarName, new File(CONF.getValue("jarPath")));
    if (!success) {
      return;
    }

    LOGGER.info("Generate DLI client to run job");
    DLIClient client = Utils.getClientByAksk(); // getClientByPassword();

    BatchJob job = null;

    try {
      LOGGER.info("Get cluster to run DLI job");
      Cluster cluster = Utils.getCluster(clusterName, client);
      while (!cluster.getStatus().equals("AVAILABLE")) {
        Thread.sleep(10 * 1000L);
        cluster = Utils.getCluster(clusterName, client);
      }

      LOGGER.info("Upload resource to run DLI job");
      PackageResource packageResource = Utils.uploadResource(jarName, "jar", bucketName, client);

      LOGGER.info("Construct DLI job info");
      SparkJobInfo jobInfo = new SparkJobInfo();
      jobInfo.setCluster_name(cluster.getClusterName());
      jobInfo.setFile(packageResource.getResourceName());
      String className = CONF.getValue("className", "com.huawei.dli.JDataPreparation");
      jobInfo.setClassName(className);
      if (className.endsWith("DataPreparation")) {
        String[] demoArgs = new String[4];
        demoArgs[0] = CONF.getAK();
        demoArgs[1] = CONF.getSK();
        demoArgs[2] = String.format("%s/%s", bucketName, "input");
        demoArgs[3] = String.format("%s/%s", bucketName, UUID.randomUUID().toString());
        jobInfo.setArgs(demoArgs);
      } else {
        jobInfo.setArgs(CONF.getValue("args").split(","));
      }

      LOGGER.info("Running DLI job");
      job = new BatchJob(cluster, jobInfo);
      job.asyncSubmit();
      LOGGER.info("Submit job success with id: {}", job.getJobId());
      while (true) {
        SparkJobStatus jobStatus = job.getStatus();
        if (SparkJobStatus.SUCCESS.equals(jobStatus)) {
          LOGGER.info("Job finished");
          return;
        }
        if (SparkJobStatus.DEAD.equals(jobStatus)) {
          throw new DLIException("The session has already exited");
        }
        Thread.sleep(1000L);
      }
    } catch (InterruptedException | DLIException e) {
      LOGGER.error("Running DLI job failed", e);
      if (null != job) {
        try {
          job.getDriverLog(0, 1000, 0).forEach(System.out::println);
        } catch (DLIException e1) {
          LOGGER.error("Get job log failed", e1);
        }
      }
    }
  }
}
