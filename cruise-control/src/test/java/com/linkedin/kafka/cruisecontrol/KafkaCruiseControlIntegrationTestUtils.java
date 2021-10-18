/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test util class.
 */
public final class KafkaCruiseControlIntegrationTestUtils {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaCruiseControlIntegrationTestUtils.class);
  
  private KafkaCruiseControlIntegrationTestUtils() {

  }
  /**
   * Find a random open port
   * @return with the port number
   */
  public static Integer findRandomOpenPortOnAllLocalInterfaces() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void waitForConditionMeet(BooleanSupplier condition, int retries, Error retriesExceededException) {
    waitForConditionMeet(condition, retries, Duration.ofSeconds(4), retriesExceededException);
  }
  /**
   * Execute boolean condition until it returns true
   * @param condition the condition to evaluate
   * @param retries the number of retries
   * @param retryBackoff the milliseconds between retries
   * @param retriesExceededException the exception if we run out of retries
   */
  public static void waitForConditionMeet(BooleanSupplier condition, int retries, Duration retryBackoff,
                                    Error retriesExceededException) {
    int counter = 0;
    while (! (counter == retries)) {
      counter++;
      boolean conditionResult = false;
      try {
        conditionResult = condition.getAsBoolean();
      } catch (Exception e) {
        LOG.warn("Exception occured", e);
      }
      if (conditionResult) {
        return;
      } else {
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    if (retriesExceededException != null) {
      throw retriesExceededException;
    }
  }
  
  public static Properties getDefaultProducerProperties(String bootstrapServers) {
    return getDefaultProducerProperties(null, bootstrapServers);
  }
  /**
   * Create default producer properties with string key and value serializer
   * @param overrides the overrides to use
   * @param bootstrapServers the bootstrap servers url
   * @return the created properties
   */
  public static Properties getDefaultProducerProperties(Properties overrides, String bootstrapServers) {
    Properties result = new Properties();

    // populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getCanonicalName());
    // apply overrides
    if (overrides != null) {
      result.putAll(overrides);
    }
    return result;
  }
  /**
   * Call cruise control REST API 
   * @param serverUrl the server base URL
   * @param path the path with get parameters
   * @return the response body as string
   */
  public static String callCruiseControl(String serverUrl, String path) {
    try {
      HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(serverUrl)
          .resolve(path).toURL().openConnection();
      String responseMessage =
          IOUtils.toString(stateEndpointConnection.getInputStream(), Charset.defaultCharset());
      return responseMessage;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
