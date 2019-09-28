package com.huawei.dli;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

public class ChangeLogLevel {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeLogLevel.class);
  private static final Map<String, Object> LOGGER_MAP = new HashMap<>();

  public void getLoggers() {
    String type = StaticLoggerBinder.getSingleton().getLoggerFactoryClassStr();
    // Currently only support slf4j+log4j.
    if ("org.slf4j.impl.Log4jLoggerFactory".equals(type)) {
      Enumeration enumeration = org.apache.log4j.LogManager.getCurrentLoggers();
      while (enumeration.hasMoreElements()) {
        org.apache.log4j.Logger logger = (org.apache.log4j.Logger) enumeration.nextElement();
        if (logger.getLevel() != null) {
          LOGGER_MAP.put(logger.getName(), logger);
        }
      }
      org.apache.log4j.Logger rootLogger = org.apache.log4j.LogManager.getRootLogger();
      LOGGER_MAP.put(rootLogger.getName(), rootLogger);
    } else {
      LOGGER.error("Unknown support log framework: {}", type);
    }
  }

  public void setAll(String logLevel) {
    for (Map.Entry<String, Object> entry : LOGGER_MAP.entrySet()) {
      org.apache.log4j.Logger targetLogger = (org.apache.log4j.Logger) entry.getValue();
      org.apache.log4j.Level targetLevel = org.apache.log4j.Level.toLevel(logLevel);
      targetLogger.setLevel(targetLevel);
    }
  }

  public void setRoot(String logLevel) {
    setSpecial("root", logLevel);
  }

  public void setSpecial(String loggerName, String logLevel) {
    for (Map.Entry<String, Object> entry : LOGGER_MAP.entrySet()) {
      if (loggerName.equals(entry.getKey())) {
        org.apache.log4j.Logger targetLogger = (org.apache.log4j.Logger) entry.getValue();
        org.apache.log4j.Level targetLevel = org.apache.log4j.Level.toLevel(logLevel);
        targetLogger.setLevel(targetLevel);
      }
    }
  }
}
