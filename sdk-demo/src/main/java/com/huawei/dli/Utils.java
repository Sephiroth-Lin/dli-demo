package com.huawei.dli;

import java.io.File;

import com.huawei.dli.sdk.Cluster;
import com.huawei.dli.sdk.DLIClient;
import com.huawei.dli.sdk.PackageResource;
import com.huawei.dli.sdk.authentication.AuthenticationMode;
import com.huawei.dli.sdk.common.DLIInfo;
import com.huawei.dli.sdk.exception.DLIException;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.PutObjectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Utils {
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);
  private static final Config CONF = new Config();

  static DLIClient getClientByPassword() {
    DLIInfo info = new DLIInfo(CONF.getRegion(), CONF.getDomainName(), CONF.getUserName(), CONF.getPassword(),
      CONF.getProjectId());
    return new DLIClient(AuthenticationMode.TOKEN, info);
  }

  static DLIClient getClientByAksk() {
    DLIInfo info = new DLIInfo(CONF.getRegion(), CONF.getAK(), CONF.getSK(), CONF.getProjectId());
    return new DLIClient(AuthenticationMode.AKSK, info);
  }

  static boolean putFileToOBS(String bucketName, String objKey, File file) {
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

  static Cluster getCluster(String clusterName, DLIClient client) throws DLIException {
    Cluster cluster;
    try {
      cluster = client.getCluster(clusterName);
    } catch (DLIException e) {
      LOGGER.warn("Get cluster failed", e);
      cluster = client.createCluster(clusterName, 4, "This is test for sdk");
    }
    return cluster;
  }

  static PackageResource uploadResource(String bucketName, String fileName, String fileType, DLIClient client)
    throws DLIException {
    String filePath = String.format("https://%s.obs.%s.myhwclouds.com/%s", bucketName, CONF.getRegion(), fileName);
    return client.uploadResources(fileType, new String[]{filePath}, "This is test for sdk").get(0);
  }
}
