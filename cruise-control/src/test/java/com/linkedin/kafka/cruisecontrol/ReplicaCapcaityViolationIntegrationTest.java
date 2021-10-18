/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See
 * License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.MinTopicLeadersPerBrokerGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.AnomalyDetectorConfig;
import com.linkedin.kafka.cruisecontrol.config.constants.MonitorConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.detector.notifier.SelfHealingNotifier;
import com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporterConfig;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCEmbeddedBroker;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.KafkaSampleStore;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import kafka.server.KafkaConfig;
import net.minidev.json.JSONArray;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC1;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;


public class ReplicaCapcaityViolationIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final int BROKER_ID_TO_ADD = 4;
  private static final int PARTITION_COUNT = 2;
  private static final int KAFKA_CLUSTER_SIZE = 4;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      "kafkacruisecontrol/" + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      "kafkacruisecontrol/" + CruiseControlEndPoint.STATE + "?substates=anomaly_detector&json=true";
  private final Configuration _gsonJsonConfig =
      Configuration.builder().jsonProvider(new JacksonJsonProvider())
          .mappingProvider(new JacksonMappingProvider()).build();

  /**
   * Remove extra brokers
   * @throws Exception
   */
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
        + KafkaCruiseControlIntegrationTestUtils.findRandomOpenPortOnAllLocalInterfaces());
    props.put(KafkaConfig.ListenersProp(), csvJoiner.toString());
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_AUTO_CREATE_CONFIG, "true");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_TOPIC_NUM_PARTITIONS_CONFIG, "1");
    
    //    props.put(CruiseControlMetricsReporterConfig.CRUISE_CONTROL_METRICS_REPORTER_INTERVAL_MS_CONFIG, "40000");
    
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

    configs.put(MonitorConfig.BROKER_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.PARTITION_METRICS_WINDOW_MS_CONFIG, "36000");
    configs.put(MonitorConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG, "3");
    configs.put(KafkaSampleStore.PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(KafkaSampleStore.BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG, "2");
    configs.put(
        TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG,
        "2");
    configs.put(AnomalyDetectorConfig.RF_SELF_HEALING_SKIP_RACK_AWARENESS_CHECK_CONFIG, "true");
    
    //    configs.put(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, "35000");
    //    configs.put(MonitorConfig.METADATA_MAX_AGE_MS_CONFIG, "35000");
    //    configs.put(MonitorConfig.METRIC_SAMPLING_INTERVAL_MS_CONFIG, "40000");
    //    configs.put(AnomalyDetectorConfig.ANOMALY_DETECTION_INTERVAL_MS_CONFIG, "150000");
    
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
    
    configs.put(AnalyzerConfig.MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    configs.put(AnalyzerConfig.OVERPROVISIONED_MAX_REPLICAS_PER_BROKER_CONFIG, "4");
    return configs;
  }

  @Test
  public void testRoleViolation() throws InterruptedException, ExecutionException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, PARTITION_COUNT, (short) 2),
          new NewTopic(TOPIC1, PARTITION_COUNT, (short) 2))).all().get();

    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
        JSONArray unfixableGoalsArray = JsonPath.<JSONArray>read(responseMessage,
            "$.AnomalyDetectorState.recentGoalViolations[*].unfixableViolatedGoals.[*]");
        List<String> unfixableGoals = JsonPath.parse(unfixableGoalsArray, _gsonJsonConfig)
            .read("$..*", new TypeRef<List<String>>() { });
        return unfixableGoals.contains("ReplicaCapacityGoal");
    }, 90, new AssertionError("Replica capacity goal violation not found"));

    Map<Object, Object> createBrokerConfig = createBrokerConfig(BROKER_ID_TO_ADD);
    CCEmbeddedBroker broker = new CCEmbeddedBroker(createBrokerConfig);
    broker.startup();
    
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
        String responseMessage = KafkaCruiseControlIntegrationTestUtils
            .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
        Integer replicaCountOnBroker = JsonPath.<Integer>read(responseMessage, "KafkaBrokerState.ReplicaCountByBrokerId."
          + BROKER_ID_TO_ADD);
        return replicaCountOnBroker > 0;
    }, 200, new AssertionError("No replica found on the new broker"));
  }

}

