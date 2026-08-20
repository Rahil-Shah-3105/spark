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

package org.apache.spark.util.kvstore;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.util.JavaUtils;

/**
 * Wrapper around types managed in a KVStore, providing easy access to their indexed fields.
 */
@Private
public class KVTypeInfo {
private static final SparkLogger LOG = SparkLoggerFactory.getLogger(KVTypeInfo.class);

  private final Class<?> type;
  private final Map<String, KVIndex> indices;
  private final Map<String, Accessor> accessors;

  public KVTypeInfo(Class<?> type) {
	LOG.debug(">>> Entering KVTypeInfo(type={})", type);
    this.type = type;
    this.accessors = new HashMap<>();
	LOG.debug("KVTypeInfo(type={}): this.type → {}", type, this.type);
	LOG.debug("KVTypeInfo(type={}): accessors → {}", type, accessors);
    this.indices = new HashMap<>();

	LOG.debug("KVTypeInfo(type={}): indices → {}", type, indices);
    for (Field f : type.getDeclaredFields()) {
      KVIndex idx = f.getAnnotation(KVIndex.class);
	LOG.debug("KVTypeInfo(type={}): idx → {}", type, idx);
      if (idx != null) {
        checkIndex(idx, indices);
        f.setAccessible(true);
        indices.put(idx.value(), idx);
        accessors.put(idx.value(), new FieldAccessor(f));
      }
    }

    for (Method m : type.getDeclaredMethods()) {
      KVIndex idx = m.getAnnotation(KVIndex.class);
      if (idx != null) {
        checkIndex(idx, indices);
        JavaUtils.checkArgument(m.getParameterCount() == 0,
          "Annotated method %s::%s should not have any parameters.", type.getName(), m.getName());
        m.setAccessible(true);
        indices.put(idx.value(), idx);
        accessors.put(idx.value(), new MethodAccessor(m));
      }
    }

    JavaUtils.checkArgument(indices.containsKey(KVIndex.NATURAL_INDEX_NAME),
        "No natural index defined for type %s.", type.getName());

    for (KVIndex idx : indices.values()) {
      if (!idx.parent().isEmpty()) {
        KVIndex parent = indices.get(idx.parent());
		LOG.debug("KVTypeInfo(type={}): parent → {}", type, parent);
        JavaUtils.checkArgument(parent != null,
          "Cannot find parent %s of index %s.", idx.parent(), idx.value());
        JavaUtils.checkArgument(parent.parent().isEmpty(),
          "Parent index %s of index %s cannot be itself a child index.", idx.parent(), idx.value());
      }
    }
	LOG.debug("<<< Exiting KVTypeInfo(type={})", type);
  }

  private void checkIndex(KVIndex idx, Map<String, KVIndex> indices) {
	LOG.debug(">>> Entering checkIndex(idx={},indices={})", idx, indices);
    JavaUtils.checkArgument(idx.value() != null && !idx.value().isEmpty(),
      "No name provided for index in type %s.", type.getName());
    JavaUtils.checkArgument(
      !idx.value().startsWith("_") || idx.value().equals(KVIndex.NATURAL_INDEX_NAME),
      "Index name %s (in type %s) is not allowed.", idx.value(), type.getName());
    JavaUtils.checkArgument(idx.parent().isEmpty() || !idx.parent().equals(idx.value()),
      "Index %s cannot be parent of itself.", idx.value());
    JavaUtils.checkArgument(!indices.containsKey(idx.value()),
      "Duplicate index %s for type %s.", idx.value(), type.getName());
	LOG.debug("<<< Exiting checkIndex(idx={},indices={})", idx, indices);
  }

  public Class<?> type() {
	LOG.debug(">>> Entering type()");
	LOG.debug("<<< Exiting type()");
    return type;
  }

  public Object getIndexValue(String indexName, Object instance) throws Exception {
	LOG.debug(">>> Entering getIndexValue(indexName={},instance={})", indexName, instance);
	LOG.debug("<<< Exiting getIndexValue(indexName={},instance={})", indexName, instance);
    return getAccessor(indexName).get(instance);
  }

  public Stream<KVIndex> indices() {
	LOG.debug(">>> Entering indices()");
	LOG.debug("<<< Exiting indices()");
    return indices.values().stream();
  }

  Accessor getAccessor(String indexName) {
	LOG.debug(">>> Entering getAccessor(indexName={})", indexName);
    Accessor a = accessors.get(indexName);
    JavaUtils.checkArgument(a != null, "No index %s.", indexName);
	LOG.debug("getAccessor(indexName={}): a → {}", indexName, a);
	LOG.debug("<<< Exiting getAccessor(indexName={})", indexName);
    return a;
  }

  Accessor getParentAccessor(String indexName) {
	LOG.debug(">>> Entering getParentAccessor(indexName={})", indexName);
    KVIndex index = indices.get(indexName);
	LOG.debug("<<< Exiting getParentAccessor(indexName={})", indexName);
    return index.parent().isEmpty() ? null : getAccessor(index.parent());
  }

  String getParentIndexName(String indexName) {
	LOG.debug(">>> Entering getParentIndexName(indexName={})", indexName);
    KVIndex index = indices.get(indexName);
	LOG.debug("<<< Exiting getParentIndexName(indexName={})", indexName);
    return index.parent();
  }

  /**
   * Abstracts the difference between invoking a Field and a Method.
   */
  interface Accessor {

    Object get(Object instance) throws ReflectiveOperationException;

    Class<?> getType();
  }

  private static class FieldAccessor implements Accessor {

    private final Field field;

    FieldAccessor(Field field) {
	LOG.debug(">>> Entering FieldAccessor(field={})", field);
      this.field = field;
	LOG.debug("<<< Exiting FieldAccessor(field={})", field);
    }

    @Override
    public Object get(Object instance) throws ReflectiveOperationException {
	LOG.debug(">>> Entering get(instance={})", instance);
	LOG.debug("<<< Exiting get(instance={})", instance);
      return field.get(instance);
    }

    @Override
    public Class<?> getType() {
	LOG.debug(">>> Entering getType()");
	LOG.debug("<<< Exiting getType()");
      return field.getType();
    }
  }

  private static class MethodAccessor implements Accessor {

    private final Method method;

    MethodAccessor(Method method) {
	LOG.debug(">>> Entering MethodAccessor(method={})", method);
      this.method = method;
	LOG.debug("<<< Exiting MethodAccessor(method={})", method);
    }

    @Override
    public Object get(Object instance) throws ReflectiveOperationException {
      return method.invoke(instance);
    }

    @Override
    public Class<?> getType() {
      return method.getReturnType();
    }
  }

}
