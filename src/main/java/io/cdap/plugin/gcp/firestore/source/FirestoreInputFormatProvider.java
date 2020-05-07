/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants;
import io.cdap.plugin.gcp.firestore.util.FirestoreConstants;
import io.cdap.plugin.gcp.firestore.util.Util;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Provides FirestoreInputFormat class name and configuration.
 */
public class FirestoreInputFormatProvider implements InputFormatProvider {

  private final Map<String, String> configMap;

  /**
   * Constructor for FirestoreInputFormatProvider object.
   * @param project  the project  of  firestore  DB
   * @param serviceAccountPath   the  service account path  of  firestore  DB
   * @param databaseId  the databaseId of  firestore  DB
   * @param collection   the collection likes a table
   * @param mode         there are two modes(basic and advanced)
   * @param pullDocuments the pull documents  for given value
   * @param skipDocuments  the skip documents for given value
   * @param filters        the filter for given field as well as value
   * @param fields         the fields of  collection
   */
  public FirestoreInputFormatProvider(String project, String serviceAccountPath, String databaseId, String collection,
                                      String mode, String pullDocuments, String skipDocuments, String filters,
                                      List<String> fields) {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>()
      .put(GCPConfig.NAME_PROJECT, project)
      .put(FirestoreConstants.PROPERTY_DATABASE_ID, Util.isNullOrEmpty(databaseId) ? "" : databaseId)
      .put(FirestoreConstants.PROPERTY_COLLECTION, Util.isNullOrEmpty(collection) ? "" : collection)
      .put(FirestoreSourceConstants.PROPERTY_QUERY_MODE, mode)
      .put(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS, Util.isNullOrEmpty(pullDocuments) ? "" : pullDocuments)
      .put(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS, Util.isNullOrEmpty(skipDocuments) ? "" : skipDocuments)
      .put(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY, Util.isNullOrEmpty(filters) ? "" : filters)
      .put(FirestoreSourceConstants.PROPERTY_SCHEMA, Joiner.on(",").join(fields));
    if (Objects.nonNull(serviceAccountPath)) {
      builder.put(GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH, serviceAccountPath);
    }
    this.configMap = builder.build();
  }

  @Override
  public String getInputFormatClassName() {
    return FirestoreInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return configMap;
  }
}
