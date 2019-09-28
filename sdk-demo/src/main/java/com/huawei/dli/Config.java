package com.huawei.dli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
  private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
  private static final Map<String, String> SETTINGS = new ConcurrentHashMap<>();

  public Config() {
    URL confUrl = this.getClass().getClassLoader().getResource("dli-env.conf");
    String file = confUrl.getFile();
    try (FileInputStream fis = new FileInputStream(file);
         InputStreamReader reader = new InputStreamReader(fis)) {
      Properties props = new Properties();
      props.load(reader);
      for (Entry<Object, Object> entry : props.entrySet()) {
        SETTINGS.putIfAbsent(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
      }
    } catch (IOException e) {
      LOGGER.error("Loading config file failed", e);
    }
  }

  public String getValue(String key) {
    return getValue(key, "Missing value");
  }

  public String getValue(String key, String defaultValue) {
    return SETTINGS.getOrDefault(key, defaultValue);
  }

  public String getRegion() {
    return getValue("region", "cn-north-1");
  }

  public String getDomainName() {
    return getValue("domainName", "unknown");
  }

  public String getUserName() {
    return getValue("userName", "unknown");
  }

  public String getProjectId() {
    return getValue("projectId", "******");
  }

  public String getAK() {
    return getValue("ak", "******");
  }

  public String getSK() {
    return getValue("sk", "******");
  }

  public String getPassword() {
    return getValue("password", "******");
  }
}
