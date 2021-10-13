/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See
 * License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.linkedin.kafka.cruisecontrol.analyzer.FixOfflineReplicaTest;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import kafka.server.KafkaConfig;
import net.minidev.json.JSONArray;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;


public class BrokerFailureIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int PARTITION_COUNT = 10;
  private static final int KAFKA_CLUSTER_SIZE = 4;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      "kafkacruisecontrol/" + CruiseControlEndPoint.KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
          "kafkacruisecontrol/" + CruiseControlEndPoint.STATE + "?substates=analyzer&json=true";
  private static final Random RANDOM = new Random(0xDEADBEEF);
  private static final Logger LOG = LoggerFactory.getLogger(FixOfflineReplicaTest.class);
  private final Configuration _gsonJsonConfig =
      Configuration.builder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).build();
  private static final int BROKER_ID_TO_REMOVE = 1;

  @Before
  public void setup() throws Exception {
    super.start();
  }

  @Override
  protected int clusterSize() {
    return KAFKA_CLUSTER_SIZE;
  }

  @After
  public void teardown() {
    super.stop();
  }

  @Override
  public Map<Object, Object> overridingProps() {
    Map<Object, Object> props = new HashMap<>();
    props.put("metric.reporters",
        "com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter");
    StringJoiner csvJoiner = new StringJoiner(",");
    csvJoiner.add(SecurityProtocol.PLAINTEXT.name + "://localhost:"
        + findRandomOpenPortOnAllLocalInterfaces());
    props.put(KafkaConfig.ListenersProp(), csvJoiner.toString());
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
    return props;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(MonitorConfig.METRIC_SAMPLER_CLASS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.monitor.sampling.CruiseControlMetricsReporterSampler");
    configs.put(AnomalyDetectorConfig.METRIC_ANOMALY_FINDER_CLASSES_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.KafkaMetricAnomalyFinder");
    configs.put(SelfHealingNotifier.SELF_HEALING_ENABLED_CONFIG, "true");
    configs.put(AnomalyDetectorConfig.TOPIC_ANOMALY_FINDER_CLASSES_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder");
    configs.put(AnomalyDetectorConfig.ANOMALY_NOTIFIER_CLASS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier");
    configs.put(SelfHealingNotifier.BROKER_FAILURE_ALERT_THRESHOLD_MS_CONFIG, "1000");
    configs.put(SelfHealingNotifier.BROKER_FAILURE_SELF_HEALING_THRESHOLD_MS_CONFIG, "1500");
    configs.put(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, "3");
    configs.put(KafkaSampleStore.PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(KafkaSampleStore.BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(
        TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG,
        "2");
    configs.put(AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG, "true");
    configs.put(
        AnomalyDetectorConfig.ANOMALY_DETECTION_GOALS_CONFIG,
        "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal," 
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal");

    String defaultGoalsValues = "com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal,"
        + "com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal";
    configs.put(AnalyzerConfig.DEFAULT_GOALS_CONFIG, defaultGoalsValues);
    
    configs.put(AnalyzerConfig.HARD_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(MinTopicLeadersPerBrokerGoal.class.getName()).toString());
    
    return configs;
  }

  @Test
  public void testBrokerFailure() throws ExecutionException, InterruptedException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2))).all().get();

    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    // wait until metadata propagates to Cruise Control
    waitForConditionMeet(() -> {
        String responseMessage = getKafkaClusterState();
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        return partitionLeaders.size() == PARTITION_COUNT;
    }, 20, new RuntimeException("Topic partitions not found for " + TOPIC0));

    produceRandomDataToTopic(TOPIC0, 4000);

    // wait for a valid proposal
    waitForConditionMeet(() -> {
      String responseMessage = getCruiseControlState();
      return JsonPath.<Boolean>read(responseMessage, "AnalyzerState.isProposalReady");
    }, 200, Duration.ofSeconds(15), new RuntimeException("No proposals were ready"));

    // shut down a broker to initiate a self-healing action
    broker(BROKER_ID_TO_REMOVE).shutdown();

    waitForConditionMeet(() -> {
        String responseMessage = getKafkaClusterState();
        Integer brokers = JsonPath.<Integer>read(responseMessage, "KafkaBrokerState.Summary.Brokers");
        JSONArray partitionLeadersArray = JsonPath.read(responseMessage,
            "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
        List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
            .read("$.*", new TypeRef<>() { });
        return partitionLeaders.size() == PARTITION_COUNT && brokers == KAFKA_CLUSTER_SIZE - 1;
    }, 200, new RuntimeException("Topic replicas not fixed after broker removed"));
  }

  private String getKafkaClusterState() {
    try {
      HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
          .resolve(CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT).toURL().openConnection();
      return IOUtils.toString(stateEndpointConnection.getInputStream(), Charset.defaultCharset());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String getCruiseControlState() {
    try {
      HttpURLConnection stateEndpointConnection = (HttpURLConnection) new URI(_app.serverUrl())
              .resolve(CRUISE_CONTROL_STATE_ENDPOINT).toURL().openConnection();
      return IOUtils.toString(stateEndpointConnection.getInputStream(), Charset.defaultCharset());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected Properties getProducerProperties(Properties overrides) {
    Properties result = new Properties();

    // populate defaults
    result.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
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

  private void produceRandomDataToTopic(String topic, int produceSize) throws ExecutionException, InterruptedException {
    if (produceSize > 0) {
      Properties props = new Properties();
      props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
      try (Producer<String, String> producer = new KafkaProducer<>(getProducerProperties(props))) {
        byte[] randomRecords = new byte[produceSize];
        RANDOM.nextBytes(randomRecords);
        producer.send(new ProducerRecord<>(topic, Arrays.toString(randomRecords))).get();
      }
    }
  }

  private Integer findRandomOpenPortOnAllLocalInterfaces() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void waitForConditionMeet(BooleanSupplier condition, int retries, RuntimeException retriesExceededException) {
    waitForConditionMeet(condition, retries, Duration.ofSeconds(4), retriesExceededException);
  }

  private void waitForConditionMeet(BooleanSupplier condition, int retries, Duration retryBackoff,
                                    RuntimeException retriesExceededException) {
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
}
