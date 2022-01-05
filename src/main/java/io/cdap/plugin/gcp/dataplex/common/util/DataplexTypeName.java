/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.common.util;

/**
 * The enum Dataplex type name.
 */
public enum DataplexTypeName {
  BOOL,
  BOOLEAN,
  BYTE,
  INT16,
  INT32,
  INT64,
  FLOAT,
  FLOAT64,
  DOUBLE,
  LONG,
  DECIMAL,
  NUMERIC,
  BIGNUMERIC,
  STRING,
  BINARY,
  BYTES,
  STRUCT,
  ARRAY,
  TIMESTAMP,
  DATE,
  TIME,
  DATETIME,
  GEOGRAPHY;

  DataplexTypeName() {
  }
}
