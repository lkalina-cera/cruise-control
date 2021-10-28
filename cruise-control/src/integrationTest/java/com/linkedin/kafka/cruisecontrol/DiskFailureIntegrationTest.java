/*
* Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See
 * License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.TypeRef;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.DiskCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaCapacityGoal;
import com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionGoal;
import com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig;
import com.linkedin.kafka.cruisecontrol.detector.TopicReplicationFactorAnomalyFinder;
import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint;
import kafka.server.KafkaConfig;
import net.minidev.json.JSONArray;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.common.TestConstants.TOPIC0;
import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.KAFKA_CLUSTER_STATE;

public class DiskFailureIntegrationTest extends CruiseControlIntegrationTestHarness {

  private static final short TOPIC0_REPLICATION_FACTOR = (short) 2;
  private static final int BROKER_ID_TO_CAUSE_DISK_FAILURE = 2;
  private static final int PARTITION_COUNT = 3;
  private static final int KAFKA_CLUSTER_SIZE = 3;
  private static final String CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT =
      "kafkacruisecontrol/" + KAFKA_CLUSTER_STATE + "?verbose=true&json=true";
  private static final String CRUISE_CONTROL_STATE_ENDPOINT =
      "kafkacruisecontrol/" + CruiseControlEndPoint.STATE + "?substates=anomaly_detector&json=true";
  private static final String CRUISE_CONTROL_ANALYZER_STATE_ENDPOINT =
      "kafkacruisecontrol/" + CruiseControlEndPoint.STATE + "?substates=analyzer&json=true";
  private final Configuration _gsonJsonConfig = KafkaCruiseControlIntegrationTestUtils.createJsonMappingConfig();
  private static final Logger LOG = LoggerFactory.getLogger(DiskFailureIntegrationTest.class);
  private List<Entry<File, File>> _brokerLogDirs = new ArrayList<>(KAFKA_CLUSTER_SIZE);
  
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
    Map<Object, Object> props = KafkaCruiseControlIntegrationTestUtils.createBrokerProps();
    Entry<File, File> logFolders = Map.entry(CCKafkaTestUtils.newTempDir(), CCKafkaTestUtils.newTempDir());
    _brokerLogDirs.add(logFolders);
    props.put(KafkaConfig.LogDirsProp(), logFolders.getKey().getAbsolutePath() + "," + logFolders.getValue().getAbsolutePath());
    return props;
  }

  @Override
  protected Map<String, Object> withConfigs() {
    Map<String, Object> configs = KafkaCruiseControlIntegrationTestUtils.ccConfigOverrides();
    configs.put(TopicReplicationFactorAnomalyFinder.SELF_HEALING_TARGET_TOPIC_REPLICATION_FACTOR_CONFIG, "2");
    
    configs.put(AnalyzerConfig.DEFAULT_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaDistributionGoal.class.getName())
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());
    
    configs.put(AnalyzerConfig.HARD_GOALS_CONFIG, new StringJoiner(",")
        .add(ReplicaCapacityGoal.class.getName())
        .add(DiskCapacityGoal.class.getName()).toString());
    
    return configs;
  }

  @Test
  public void testDiskFailure() throws IOException {
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(Collections
        .singletonMap(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker(0).plaintextAddr()));
    try {
      adminClient.createTopics(Arrays.asList(new NewTopic(TOPIC0, PARTITION_COUNT, TOPIC0_REPLICATION_FACTOR)));

    } finally {
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }

    waitForMetadataPropogates();

    KafkaCruiseControlIntegrationTestUtils.produceRandomDataToTopic(TOPIC0, 5, 400, KafkaCruiseControlIntegrationTestUtils
        .getDefaultProducerProperties(bootstrapServers()));

    waitForProposal();
    
    Entry<File, File> entry = _brokerLogDirs.get(BROKER_ID_TO_CAUSE_DISK_FAILURE);
    FileUtils.deleteDirectory(entry.getKey());
    LOG.info("Disk failure injected");
    waitForOfflineReplicaDetection();
    
    waitForDiskFailureDetection();
    
    waitForOfflineReplicasFixed();
    
  }

  private void waitForOfflineReplicasFixed() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      Map<String, String> offlineReplicas = JsonPath.read(responseMessage,
            "$.KafkaBrokerState.OfflineReplicaCountByBrokerId");
      return offlineReplicas.isEmpty();
    }, 800, new AssertionError("There are still offline replica on broker"));
  }

  private void waitForDiskFailureDetection() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_STATE_ENDPOINT);
      JSONArray diskFailuresArray = JsonPath.read(responseMessage,
            "$.AnomalyDetectorState.recentDiskFailures[*].anomalyId");
      return diskFailuresArray.size() == 1;
    }, 400, new AssertionError("Disk failure anomaly not detected"));
  }

  private void waitForOfflineReplicaDetection() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      Integer offlineReplicas = JsonPath.read(responseMessage,
          "$.KafkaBrokerState.OfflineReplicaCountByBrokerId." + BROKER_ID_TO_CAUSE_DISK_FAILURE);
      return offlineReplicas > 0;
    }, 400, new AssertionError("Offline replicas not detected"));
  }

  private void waitForProposal() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_ANALYZER_STATE_ENDPOINT);
      return JsonPath.<Boolean>read(responseMessage, "AnalyzerState.isProposalReady");
    }, Duration.ofSeconds(200), Duration.ofSeconds(15), new AssertionError("No proposal available"));
  }

  private void waitForMetadataPropogates() {
    KafkaCruiseControlIntegrationTestUtils.waitForConditionMeet(() -> {
      String responseMessage = KafkaCruiseControlIntegrationTestUtils
          .callCruiseControl(_app.serverUrl(), CRUISE_CONTROL_KAFKA_CLUSTER_STATE_ENDPOINT);
      JSONArray partitionLeadersArray = JsonPath.<JSONArray>read(responseMessage,
          "$.KafkaPartitionState.other[?(@.topic == '" + TOPIC0 + "')].leader");
      List<Integer> partitionLeaders = JsonPath.parse(partitionLeadersArray, _gsonJsonConfig)
          .read("$.*", new TypeRef<List<Integer>>() { });
      return partitionLeaders.size() == PARTITION_COUNT;
    }, 80, new AssertionError("Topic partitions not found for " + TOPIC0));
  }

}

