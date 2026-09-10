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

package org.apache.spark.util;

/**
 * A class loader which makes some protected methods in ClassLoader accessible.
 */
public class ParentClassLoader extends ClassLoader {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(ParentClassLoader.class);

  static {
    ClassLoader.registerAsParallelCapable();
  }

  public ParentClassLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Class<?> findClass(String name) throws ClassNotFoundException {
	logger.warn("findClass(name)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", name);
    return super.findClass(name);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
	logger.warn("loadClass(name,resolve)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", name, resolve);
    return super.loadClass(name, resolve);
  }
}
