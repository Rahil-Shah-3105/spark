/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Provides a mechanism for constructing a {@link TransportConf} using some sort of configuration.
 */
public abstract class ConfigProvider {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(ConfigProvider.class);
  /** Obtains the value of the given config, throws NoSuchElementException if it doesn't exist. */
  public abstract String get(String name);

  /** Returns all the config values in the provider. */
  public abstract Iterable<Map.Entry<String, String>> getAll();

  public String get(String name, String defaultValue) {
    try {
      return get(name);
	logger.warn("get(name,defaultValue)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, defaultValue);
    } catch (NoSuchElementException e) {
      return defaultValue;
    }
  }

  public int getInt(String name, int defaultValue) {
	logger.warn("getInt(name,defaultValue)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, defaultValue);
    return Integer.parseInt(get(name, Integer.toString(defaultValue)));
  }

  public long getLong(String name, long defaultValue) {
	logger.warn("getLong(name,defaultValue)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, defaultValue);
    return Long.parseLong(get(name, Long.toString(defaultValue)));
  }

  public double getDouble(String name, double defaultValue) {
	logger.warn("getDouble(name,defaultValue)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, defaultValue);
    return Double.parseDouble(get(name, Double.toString(defaultValue)));
  }

  public boolean getBoolean(String name, boolean defaultValue) {
	logger.warn("getBoolean(name,defaultValue)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, defaultValue);
    return Boolean.parseBoolean(get(name, Boolean.toString(defaultValue)));
  }

}
