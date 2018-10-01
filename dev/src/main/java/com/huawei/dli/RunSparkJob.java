package com.huawei.dli;

import java.io.File;
import java.util.UUID;

import com.huawei.dli.sdk.*;
import com.huawei.dli.sdk.authentication.AuthenticationMode;
import com.huawei.dli.sdk.common.DLIInfo;
import com.huawei.dli.sdk.common.SparkJobInfo;
import com.huawei.dli.sdk.exception.DLIException;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.PutObjectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunSparkJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(RunSparkJob.class);
  private static final Config CONF = new Config();

  public static void main(String[] args) {
    String bucketName = CONF.getValue("bucketName");
    String jarName = CONF.getValue("jarName", "dli-demo-1.0.0.jar");
    String clusterName = CONF.getValue("clusterName", "4cu_sdk");

    LOGGER.info("Put resources to OBS");
    boolean success = putFileToOBS(bucketName, jarName, new File(CONF.getValue("jarPath")));
    if (!success) {
      return;
    }

    LOGGER.info("Generate DLI client to run job");
    DLIClient client = getClientByPassword();

    BatchJob job = null;

    try {
      LOGGER.info("Get cluster to run DLI job");
      Cluster cluster = getCluster(clusterName, client);

      LOGGER.info("Upload resource to run DLI job");
      PackageResource packageResource = uploadResource(jarName, bucketName, client);

      LOGGER.info("Construct DLI job info");
      SparkJobInfo jobInfo = new SparkJobInfo();
      jobInfo.setCluster_name(cluster.getClusterName());
      jobInfo.setFile(packageResource.getResourceName());
      String className = CONF.getValue("className", "com.huawei.dli.DataPreparation");
      jobInfo.setClassName(className);
      if (className.endsWith("DataPreparation")) {
        String[] demoArgs = new String[2];
        demoArgs[0] = String.format("%s/%s", bucketName, "input");
        demoArgs[1] = String.format("%s/%s", bucketName, UUID.randomUUID().toString());
        jobInfo.setArgs(demoArgs);
      }

      LOGGER.info("Running DLI job");
      job = new BatchJob(cluster, jobInfo);
      // At default, job timeout is -1, so will waiting job until finished.
      job.submit();
    } catch (DLIException e) {
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

  private static DLIClient getClientByPassword() {
    DLIInfo info = new DLIInfo(CONF.getRegion(), CONF.getDomainName(), CONF.getUserName(), CONF.getPassword(),
      CONF.getProjectId());
    return new DLIClient(AuthenticationMode.TOKEN, info);
  }

  private static boolean putFileToOBS(String bucketName, String objKey, File file) {
    boolean success = false;
    String endpoint = "obs.myhwclouds.com";
    ObsClient client = new ObsClient(CONF.getAK(), CONF.getSK(), endpoint);
    try {
      PutObjectResult result = client.putObject(bucketName, objKey, file);
      success = true;
      LOGGER.info("Put file to OBS success, result is: " + result.toString());
    } catch (ObsException e) {
      LOGGER.error("Put file to OBS failed", e);
    }
    return success;
  }

  private static Cluster getCluster(String clusterName, DLIClient client) throws DLIException {
    Cluster cluster;
    try {
      cluster = client.getCluster(clusterName);
    } catch (DLIException e) {
      LOGGER.warn("Get cluster failed", e);
      cluster = client.createCluster(clusterName, 4, "This is test for sdk");
    }
    return cluster;
  }

  private static PackageResource uploadResource(String jarName, String bucketName, DLIClient client)
    throws DLIException {
    String jarPath = String.format("https://%s.obs.%s.myhwclouds.com/%s", bucketName, CONF.getRegion(), jarName);
    return client.uploadResources("jar", new String[]{jarPath}, "This is test for sdk").get(0);
  }
}
