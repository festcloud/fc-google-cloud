/*
 * Copyright © 2015-2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.gcs.actions;

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.batch.action.Condition;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Cmek Key unit test for GCSBucketCreate.Config, SourceDestConfig used by GCSCopy and GCSMove and
 * GCSDoneFileMarker.Config.
 * This test will only be run when below property is provided:
 * project.id -- the name of the project where staging bucket may be created or new resource needs to be created.
 * It will default to active google project if you have google cloud client installed.
 * service.account.file -- the path to the service account key file
 */
public class GCSActionsCmekKeyTest {
  private static String serviceAccountKey;
  private static String project;
  private static String serviceAccountFilePath;
  private static String destPath;
  private static Storage storage;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    // Certain properties need to be configured otherwise the whole tests will be skipped.
    String messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";

    project = System.getProperty("project.id");
    if (project == null) {
      project = System.getProperty("GOOGLE_CLOUD_PROJECT");
    }
    if (project == null) {
      project = System.getProperty("GCLOUD_PROJECT");
    }
    Assume.assumeFalse(String.format(messageTemplate, "project id"), project == null);
    System.setProperty("GCLOUD_PROJECT", project);

    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);

    destPath = getDestPath(project);

    serviceAccountKey = new String(Files.readAllBytes(Paths.get(new File(serviceAccountFilePath).getAbsolutePath())),
                                   StandardCharsets.UTF_8);
    Credentials credentials = GCPUtils.loadServiceAccountCredentials(serviceAccountKey, false);
    storage = GCPUtils.getStorage(project, credentials);
  }

  //This method creates a unique GCS bucket path.
  private static String getDestPath(@Nullable String project) {
    return String.format("gs://gcs-cmektest-%s%s", project,
                         new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss").format(new Date()));
  }

  private GCSBucketCreate.Config.Builder getGCSBucketCreateConfigBuilder() {
    return GCSBucketCreate.Config.builder()
      .setProject(project)
      .setGcsPath(destPath);
  }

  private SourceDestConfig.Builder getSourceDestConfigBuilder() {
    return SourceDestConfig.builder()
      .setProject(project)
      .setGcsPath(destPath);
  }

  private GCSDoneFileMarker.Config.Builder getGCSDoneFileMarkerBuilder() {
    return GCSDoneFileMarker.Config.builder()
      .setProject(project)
      .setGcsPath(destPath);
  }

  private GCSBucketCreate.Config getConfig(GCSBucketCreate.Config.Builder builder, String key,
                                           @Nullable String location) {
    return builder
      .setCmekKey(key)
      .setLocation(location)
      .build();
  }

  private SourceDestConfig getConfig(SourceDestConfig.Builder builder, String key,
                                     @Nullable String location) {
    return builder
      .setCmekKey(key)
      .setLocation(location)
      .build();
  }

  private GCSDoneFileMarker.Config getConfig(GCSDoneFileMarker.Config.Builder builder, String key,
                                             String runCondition, @Nullable String location) {
    return builder
      .setCmekKey(key)
      .setRunCondition(runCondition)
      .setLocation(location)
      .build();
  }

  @Test
  public void testServiceAccountPath() {
    GCSBucketCreate.Config.Builder gcsBucketCreateBuilder = getGCSBucketCreateConfigBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_FILE_PATH)
      .setServiceFilePath(serviceAccountFilePath);
    SourceDestConfig.Builder sourceDestConfigBuilder = getSourceDestConfigBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_FILE_PATH)
      .setServiceFilePath(serviceAccountFilePath);
    GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder = getGCSDoneFileMarkerBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_FILE_PATH)
      .setServiceFilePath(serviceAccountFilePath);

    testValidCmekKey(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testInvalidCmekKeyName(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testInvalidCmekKeyLocation(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testCmekKeyWithExistingBucket(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
  }

  @Test
  public void testServiceAccountJson() {
    GCSBucketCreate.Config.Builder gcsBucketCreateBuilder = getGCSBucketCreateConfigBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_JSON)
      .setServiceAccountJson(serviceAccountKey);
    SourceDestConfig.Builder sourceDestConfigBuilder = getSourceDestConfigBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_JSON)
      .setServiceAccountJson(serviceAccountKey);
    GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder = getGCSDoneFileMarkerBuilder()
      .setServiceAccountType(GCPConfig.SERVICE_ACCOUNT_JSON)
      .setServiceAccountJson(serviceAccountKey);

    testValidCmekKey(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testInvalidCmekKeyName(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testInvalidCmekKeyLocation(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
    testCmekKeyWithExistingBucket(gcsBucketCreateBuilder, sourceDestConfigBuilder, gcsDoneFileMarkerBuilder);
  }

  private void testValidCmekKey(GCSBucketCreate.Config.Builder gcsBucketCreateBuilder,
                                SourceDestConfig.Builder sourceDestConfigBuilder,
                                GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder) {
    MockFailureCollector collector = new MockFailureCollector();
    String configKey = String.format("projects/%s/locations/key-location/keyRings/ring/cryptoKeys/test_key", project);
    String location = "key-location";
    GCSBucketCreate.Config gcsBucketCreateConfig = getConfig(gcsBucketCreateBuilder, configKey, location);
    gcsBucketCreateConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());

    SourceDestConfig sourceDestConfig = getConfig(sourceDestConfigBuilder, configKey, location);
    sourceDestConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());

    String runCondition = Condition.SUCCESS.name();
    GCSDoneFileMarker.Config gcsDoneFileMarkerConfig = getConfig(gcsDoneFileMarkerBuilder, configKey,
                                                                 runCondition, location);
    gcsDoneFileMarkerConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private void testInvalidCmekKeyName(GCSBucketCreate.Config.Builder gcsBucketCreateBuilder,
                                      SourceDestConfig.Builder sourceDestConfigBuilder,
                                      GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder) {
    MockFailureCollector collector = new MockFailureCollector();
    String configKey = String.format("projects/%s/locations/key-location/keyRings", project);
    String location = "bucket-location";
    GCSBucketCreate.Config gcsBucketCreateConfig = getConfig(gcsBucketCreateBuilder, configKey, location);
    gcsBucketCreateConfig.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    SourceDestConfig sourceDestConfig = getConfig(sourceDestConfigBuilder, configKey, location);
    sourceDestConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    String runCondition = Condition.SUCCESS.name();
    GCSDoneFileMarker.Config gcsDoneFileMarkerConfig = getConfig(gcsDoneFileMarkerBuilder, configKey,
                                                                 runCondition, location);
    gcsDoneFileMarkerConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(2, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testInvalidCmekKeyLocation(GCSBucketCreate.Config.Builder gcsBucketCreateBuilder,
                                          SourceDestConfig.Builder sourceDestConfigBuilder,
                                          GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder) {
    MockFailureCollector collector = new MockFailureCollector();
    String configKey = String.format("projects/%s/locations/key-location/keyRings/ring/cryptoKeys/test_key", project);
    String location = "bucket-location";
    GCSBucketCreate.Config gcsBucketCreateConfig = getConfig(gcsBucketCreateBuilder, configKey, location);
    gcsBucketCreateConfig.validateCmekKey(collector, Collections.emptyMap());
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    //testing the default location of the bucket ("US") if location config is empty or null
    gcsBucketCreateConfig = getConfig(gcsBucketCreateBuilder, configKey, null);
    gcsBucketCreateConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    SourceDestConfig sourceDestConfig = getConfig(sourceDestConfigBuilder, configKey, location);
    sourceDestConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(2);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    //testing the default location of the bucket ("US") if location config is empty or null
    sourceDestConfig = getConfig(sourceDestConfigBuilder, configKey, null);
    sourceDestConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(3);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    String runCondition = Condition.SUCCESS.name();
    GCSDoneFileMarker.Config gcsDoneFileMarkerConfig = getConfig(gcsDoneFileMarkerBuilder, configKey,
                                                                 runCondition, location);
    gcsDoneFileMarkerConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(4);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    //testing the default location of the bucket ("US") if location config is empty or null
    gcsDoneFileMarkerConfig = getConfig(gcsDoneFileMarkerBuilder, configKey, runCondition, null);
    gcsDoneFileMarkerConfig.validateCmekKey(collector, Collections.emptyMap());
    failure = collector.getValidationFailures().get(5);
    causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("cmekKey", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  private void testCmekKeyWithExistingBucket(GCSBucketCreate.Config.Builder gcsBucketCreateBuilder,
                                             SourceDestConfig.Builder sourceDestConfigBuilder,
                                             GCSDoneFileMarker.Config.Builder gcsDoneFileMarkerBuilder) {
    String cmekKey = String.format("projects/%s/locations/key-location/keyRings/my_ring/cryptoKeys/test_key", project);
    MockFailureCollector collector = new MockFailureCollector();
    // Even though the location set in config is not same as of the cmek key but the validation should not fail because
    // the bucket already exists.
    GCSBucketCreate.Config gcsBucketCreateConfig = getConfig(gcsBucketCreateBuilder, cmekKey, null);
    
    // creating bucket before validating cmek key.
    Credentials credentials = gcsBucketCreateConfig.getCredentials(collector);
    Storage storage = GCPUtils.getStorage(project, credentials);
    GCPUtils.createBucket(storage, GCSPath.from(destPath).getBucket(), "us-east1", null);

    gcsBucketCreateConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());

    SourceDestConfig sourceDestConfig = getConfig(sourceDestConfigBuilder, cmekKey, null);
    sourceDestConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());

    String runCondition = Condition.SUCCESS.name();
    GCSDoneFileMarker.Config gcsDoneFileMarkerConfig = getConfig(gcsDoneFileMarkerBuilder, cmekKey, runCondition, null);
    gcsDoneFileMarkerConfig.validateCmekKey(collector, Collections.emptyMap());
    Assert.assertEquals(0, collector.getValidationFailures().size());
    // deleting bucket after successful validation
    storage.delete(GCSPath.from(destPath).getBucket());
  }
}
