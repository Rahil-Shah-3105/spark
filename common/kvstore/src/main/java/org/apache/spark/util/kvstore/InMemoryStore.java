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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.annotation.Private;
import org.apache.spark.network.util.JavaUtils;

/**
 * Implementation of KVStore that keeps data deserialized in memory. This store does not index
 * data; instead, whenever iterating over an indexed field, the stored data is copied and sorted
 * according to the index. This saves memory but makes iteration more expensive.
 */
@Private
public class InMemoryStore implements KVStore {
private static final SparkLogger LOG = SparkLoggerFactory.getLogger(InMemoryStore.class);

  private Object metadata;
  private InMemoryLists inMemoryLists = new InMemoryLists();

  @Override
  public <T> T getMetadata(Class<T> klass) {
	LOG.debug(">>> Entering getMetadata(klass={})", klass);
	LOG.debug("<<< Exiting getMetadata(klass={})", klass);
    return klass.cast(metadata);
  }

  @Override
  public void setMetadata(Object value) {
	LOG.debug(">>> Entering setMetadata(value={})", value);
    this.metadata = value;
	LOG.debug("<<< Exiting setMetadata(value={})", value);
  }

  @Override
  public long count(Class<?> type) {
	LOG.debug(">>> Entering count(type={})", type);
    InstanceList<?> list = inMemoryLists.get(type);
	LOG.debug("count(type={}): list → {}", type, list);
    return list != null ? list.size() : 0;
  }

  @Override
  public long count(Class<?> type, String index, Object indexedValue) throws Exception {
    InstanceList<?> list = inMemoryLists.get(type);
    if (list == null) {
      // The type has never been written, so there are no matching entities. Still validate
      // the index name (via reflection on the type) before returning, so that an unknown
      // index fails the same way it does for a populated type and for the LevelDB/RocksDB
      // stores, instead of being silently swallowed.
      new KVTypeInfo(type).getAccessor(index);
      return 0;
    }
    int count = 0;
    Object comparable = asKey(indexedValue);
    KVTypeInfo.Accessor accessor = list.getIndexAccessor(index);
    for (Object o : view(type)) {
      if (Objects.equals(comparable, asKey(accessor.get(o)))) {
        count++;
      }
    }
    return count;
  }

  @Override
  public <T> T read(Class<T> klass, Object naturalKey) {
	LOG.debug(">>> Entering read(klass={},naturalKey={})", klass, naturalKey);
    InstanceList<T> list = inMemoryLists.get(klass);
    T value = list != null ? list.get(naturalKey) : null;
	LOG.debug("read(klass={},naturalKey={}): value → {}", klass, naturalKey, value);
	LOG.debug("read(klass={},naturalKey={}): list → {}", klass, naturalKey, list);
    if (value == null) {
	LOG.warn("Throwing NoSuchElementException from read(klass={},naturalKey={})", klass, naturalKey);
	LOG.debug("<<< Exiting read(klass={},naturalKey={})", klass, naturalKey);
      throw new NoSuchElementException();
    }
    return value;
  }

  @Override
  public void write(Object value) throws Exception {
    inMemoryLists.write(value);
	LOG.debug(">>> Entering write(value={})", value);
	LOG.debug("<<< Exiting write(value={})", value);
  }

  @Override
  public void delete(Class<?> type, Object naturalKey) {
	LOG.debug(">>> Entering delete(type={},naturalKey={})", type, naturalKey);
    InstanceList<?> list = inMemoryLists.get(type);
    if (list != null) {
	LOG.debug("delete(type={},naturalKey={}): list → {}", type, naturalKey, list);
      list.delete(naturalKey);
    }
	LOG.debug("<<< Exiting delete(type={},naturalKey={})", type, naturalKey);
  }

  @Override
  public <T> KVStoreView<T> view(Class<T> type){
	LOG.debug(">>> Entering view(type={})", type);
    InstanceList<T> list = inMemoryLists.get(type);
	LOG.debug("view(type={}): list → {}", type, list);
	LOG.debug("<<< Exiting view(type={})", type);
    return list != null ? list.view() : emptyView();
  }

  @Override
  public void close() {
	LOG.debug(">>> Entering close()");
    metadata = null;
	LOG.debug("close(): metadata → {}", metadata);
    inMemoryLists.clear();
	LOG.debug("<<< Exiting close()");
  }

  @Override
  public <T> boolean removeAllByIndexValues(
      Class<T> klass,
      String index,
      Collection<?> indexValues) {
	LOG.debug(">>> Entering removeAllByIndexValues(klass={},index={},indexValues={})", klass, index, indexValues);
    InstanceList<T> list = inMemoryLists.get(klass);

	LOG.debug("removeAllByIndexValues(klass={},index={},indexValues={}): list → {}", klass, index, indexValues, list);
    if (list != null) {
      return list.countingRemoveAllByIndexValues(index, indexValues) > 0;
    } else {
	LOG.debug("<<< Exiting removeAllByIndexValues(klass={},index={},indexValues={})", klass, index, indexValues);
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static Comparable<Object> asKey(Object in) {
	LOG.debug(">>> Entering asKey(in={})", in);
    if (in.getClass().isArray()) {
      in = ArrayWrappers.forArray(in);
	LOG.debug("asKey(in={}): in → {}", in, in);
    }
	LOG.debug("<<< Exiting asKey(in={})", in);
    return (Comparable<Object>) in;
  }

  @SuppressWarnings("unchecked")
  private static <T> KVStoreView<T> emptyView() {
	LOG.debug(">>> Entering emptyView()");
	LOG.debug("<<< Exiting emptyView()");
    return (InMemoryView<T>) InMemoryView.EMPTY_VIEW;
  }

  /**
   * Encapsulates ConcurrentHashMap so that the typing in and out of the map strictly maps a
   * class of type T to an InstanceList of type T.
   */
  private static class InMemoryLists {
    private final ConcurrentMap<Class<?>, InstanceList<?>> data = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> InstanceList<T> get(Class<T> type) {
	LOG.debug(">>> Entering get(type={})", type);
	LOG.debug("<<< Exiting get(type={})", type);
      return (InstanceList<T>) data.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T> void write(T value) throws Exception {
      InstanceList<T> list =
        (InstanceList<T>) data.computeIfAbsent(value.getClass(), InstanceList::new);
      list.put(value);
    }

    public void clear() {
	LOG.debug(">>> Entering clear()");
      data.clear();
	LOG.debug("<<< Exiting clear()");
    }
  }

  /**
   * An alias class for the type "{@literal ConcurrentHashMap<Comparable<Object>, Boolean>}",
   * which is used as a concurrent hashset for storing natural keys
   * and the boolean value doesn't matter.
   */
  private static class NaturalKeys extends ConcurrentHashMap<Comparable<Object>, Boolean> {}

  private static class InstanceList<T> {

    /**
     * A BiConsumer to control multi-entity removal.  We use this in a forEach rather than an
     * iterator because there is a bug in jdk8 which affects remove() on all concurrent map
     * iterators.  https://bugs.openjdk.java.net/browse/JDK-8078645
     */
    private static class CountingRemoveIfForEach<T> implements BiConsumer<Comparable<Object>, T> {
      private final InstanceList<T> instanceList;
      private final Predicate<? super T> filter;

      /**
       * Keeps a count of the number of elements removed.  This count is not currently surfaced
       * to clients of KVStore as Java's generic removeAll() construct returns only a boolean,
       * but I found it handy to have the count of elements removed while debugging; a count being
       * no more complicated than a boolean, I've retained that behavior here, even though there
       * is no current requirement.
       */
      private int count = 0;

      CountingRemoveIfForEach(InstanceList<T> instanceList, Predicate<? super T> filter) {
		LOG.debug(">>> Entering CountingRemoveIfForEach(instanceList={},filter={})", instanceList, filter);
        this.instanceList = instanceList;
		LOG.debug("CountingRemoveIfForEach(instanceList={},filter={}): this.instanceList → {}", instanceList, filter, this.instanceList);
        this.filter = filter;
		LOG.debug("<<< Exiting CountingRemoveIfForEach(instanceList={},filter={})", instanceList, filter);
      }

      @Override
      public void accept(Comparable<Object> key, T value) {
		LOG.debug(">>> Entering accept(key={},value={})", key, value);
        if (filter.test(value)) {
          if (instanceList.delete(key, value)) {
            count++;
          }
        }
		LOG.debug("<<< Exiting accept(key={},value={})", key, value);
      }

      public int count() { return count; }
    }

    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor naturalKey;
    private final ConcurrentMap<Comparable<Object>, T> data;
    private final String naturalParentIndexName;
    private final Boolean hasNaturalParentIndex;
    // A mapping from parent to the natural keys of its children.
    // For example, a mapping from a stage ID to all the task IDs in the stage.
    private final ConcurrentMap<Comparable<Object>, NaturalKeys> parentToChildrenMap;

    private InstanceList(Class<?> klass) {
	LOG.debug(">>> Entering InstanceList(klass={})", klass);
	LOG.debug("InstanceList(klass={}): naturalKey → {}", klass, naturalKey);
	LOG.debug("InstanceList(klass={}): data → {}", klass, data);
      this.ti = new KVTypeInfo(klass);
      this.naturalKey = ti.getAccessor(KVIndex.NATURAL_INDEX_NAME);
      this.data = new ConcurrentHashMap<>();
      this.naturalParentIndexName = ti.getParentIndexName(KVIndex.NATURAL_INDEX_NAME);
	LOG.debug("InstanceList(klass={}): naturalParentIndexName → {}", klass, naturalParentIndexName);
      this.parentToChildrenMap = new ConcurrentHashMap<>();
	LOG.debug("InstanceList(klass={}): parentToChildrenMap → {}", klass, parentToChildrenMap);
      this.hasNaturalParentIndex = !naturalParentIndexName.isEmpty();
	LOG.debug("<<< Exiting InstanceList(klass={})", klass);
    }

    KVTypeInfo.Accessor getIndexAccessor(String indexName) {
	LOG.debug(">>> Entering getIndexAccessor(indexName={})", indexName);
	LOG.debug("<<< Exiting getIndexAccessor(indexName={})", indexName);
      return ti.getAccessor(indexName);
    }

    int countingRemoveAllByIndexValues(String index, Collection<?> indexValues) {
	LOG.debug(">>> Entering countingRemoveAllByIndexValues(index={},indexValues={})", index, indexValues);
      int count = 0;
      if (KVIndex.NATURAL_INDEX_NAME.equals(index)) {
		LOG.debug("countingRemoveAllByIndexValues(index={},indexValues={}): count → {}", index, indexValues, count);
        for (Object naturalKey : indexValues) {
          count += delete(asKey(naturalKey)) ? 1 : 0;
        }
        return count;
      } else if (hasNaturalParentIndex && naturalParentIndexName.equals(index)) {
        // If there is a parent index for the natural index and `index` happens to be it,
        // Spark can use the `parentToChildrenMap` to get the related natural keys, and then
        // delete them from `data`.
        for (Object indexValue : indexValues) {
          Comparable<Object> parentKey = asKey(indexValue);
		LOG.debug("countingRemoveAllByIndexValues(index={},indexValues={}): parentKey → {}", index, indexValues, parentKey);
          NaturalKeys children = parentToChildrenMap.get(parentKey);
		LOG.debug("countingRemoveAllByIndexValues(index={},indexValues={}): children → {}", index, indexValues, children);
          if (children == null) {
            continue;
          }
          for (Comparable<Object> naturalKey : children.keySet()) {
            data.remove(naturalKey);
            count ++;
          }
          parentToChildrenMap.remove(parentKey);
        }
        return count;
      } else {
        Predicate<? super T> filter = getPredicate(ti.getAccessor(index), indexValues);
		LOG.debug("countingRemoveAllByIndexValues(index={},indexValues={}): filter → {}", index, indexValues, filter);
        CountingRemoveIfForEach<T> callback = new CountingRemoveIfForEach<>(this, filter);
		LOG.debug("countingRemoveAllByIndexValues(index={},indexValues={}): callback → {}", index, indexValues, callback);

        // Go through all the values in `data` and delete objects that meets the predicate `filter`.
        // This can be slow when there is a large number of entries in `data`.
        data.forEach(callback);
		LOG.debug("<<< Exiting countingRemoveAllByIndexValues(index={},indexValues={})", index, indexValues);
        return callback.count();
      }
    }

    public T get(Object key) {
      return data.get(asKey(key));
    }

    public void put(T value) throws Exception {
	LOG.debug(">>> Entering put(value={})", value);
      data.put(asKey(naturalKey.get(value)), value);
      if (hasNaturalParentIndex) {
        Comparable<Object> parentKey = asKey(getIndexAccessor(naturalParentIndexName).get(value));
		LOG.debug("put(value={}): parentKey → {}", value, parentKey);
        NaturalKeys children =
          parentToChildrenMap.computeIfAbsent(parentKey, k -> new NaturalKeys());
        children.put(asKey(naturalKey.get(value)), true);
      }
	LOG.debug("<<< Exiting put(value={})", value);
    }

    public boolean delete(Object key) {
      boolean entryExists = data.remove(asKey(key)) != null;
      if (entryExists) {
        deleteParentIndex(key);
      }
      return entryExists;
    }

    public boolean delete(Object key, T value) {
      boolean entryExists = data.remove(asKey(key), value);
      if (entryExists) {
        deleteParentIndex(key);
      }
      return entryExists;
    }

    private void deleteParentIndex(Object key) {
	LOG.debug(">>> Entering deleteParentIndex(key={})", key);
      if (hasNaturalParentIndex) {
        for (NaturalKeys v : parentToChildrenMap.values()) {
          if (v.remove(asKey(key)) != null) {
            // `v` can be empty after removing the natural key and we can remove it from
            // `parentToChildrenMap`. However, `parentToChildrenMap` is a ConcurrentMap and such
            // checking and deleting can be slow.
            // This method is to delete one object with certain key, let's make it simple here.
            break;
          }
        }
      }
	LOG.debug("<<< Exiting deleteParentIndex(key={})", key);
    }

    public int size() {
	LOG.debug(">>> Entering size()");
	LOG.debug("<<< Exiting size()");
      return data.size();
    }

    public InMemoryView<T> view() {
      return new InMemoryView<>(data, ti, naturalParentIndexName, parentToChildrenMap);
    }

    private static <T> Predicate<? super T> getPredicate(
        KVTypeInfo.Accessor getter,
        Collection<?> values) {
      if (Comparable.class.isAssignableFrom(getter.getType())) {
        HashSet<?> set = new HashSet<>(values);

        return (value) -> set.contains(indexValueForEntity(getter, value));
      } else {
        HashSet<Comparable<?>> set = new HashSet<>(values.size());
        for (Object key : values) {
          set.add(asKey(key));
        }
        return (value) -> set.contains(asKey(indexValueForEntity(getter, value)));
      }
    }

    private static Object indexValueForEntity(KVTypeInfo.Accessor getter, Object entity) {
	LOG.debug(">>> Entering indexValueForEntity(getter={},entity={})", getter, entity);
      try {
		LOG.debug("<<< Exiting indexValueForEntity(getter={},entity={})", getter, entity);
        return getter.get(entity);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class InMemoryView<T> extends KVStoreView<T> {
    private static final InMemoryView<?> EMPTY_VIEW =
      new InMemoryView<>(new ConcurrentHashMap<>(), null, "", new ConcurrentHashMap<>());

    private final ConcurrentMap<Comparable<Object>, T> data;
    private final KVTypeInfo ti;
    private final KVTypeInfo.Accessor natural;
    private final ConcurrentMap<Comparable<Object>, NaturalKeys> parentToChildrenMap;
    private final String naturalParentIndexName;
    private final Boolean hasNaturalParentIndex;

    InMemoryView(
        ConcurrentMap<Comparable<Object>, T> data,
        KVTypeInfo ti,
        String naturalParentIndexName,
        ConcurrentMap<Comparable<Object>, NaturalKeys> parentToChildrenMap) {
		LOG.debug("InMemoryView(data={},ti={},naturalParentIndexName={},parentToChildrenMap={}): parentToChildrenMap → {}", data, ti, naturalParentIndexName, parentToChildrenMap, parentToChildrenMap);
		LOG.debug("InMemoryView(data={},ti={},naturalParentIndexName={},parentToChildrenMap={}): this.naturalParentIndexName → {}", data, ti, naturalParentIndexName, parentToChildrenMap, this.naturalParentIndexName);
      this.data = data;
      this.ti = ti;
	LOG.debug(">>> Entering InMemoryView(data={},ti={},naturalParentIndexName={},parentToChildrenMap={})", data, ti, naturalParentIndexName, parentToChildrenMap);
      this.natural = ti != null ? ti.getAccessor(KVIndex.NATURAL_INDEX_NAME) : null;
      this.naturalParentIndexName = naturalParentIndexName;
	LOG.debug("InMemoryView(data={},ti={},naturalParentIndexName={},parentToChildrenMap={}): this.data → {}", data, ti, naturalParentIndexName, parentToChildrenMap, this.data);
      this.parentToChildrenMap = parentToChildrenMap;
      this.hasNaturalParentIndex = !naturalParentIndexName.isEmpty();
	LOG.debug("InMemoryView(data={},ti={},naturalParentIndexName={},parentToChildrenMap={}): this.ti → {}", data, ti, naturalParentIndexName, parentToChildrenMap, this.ti);
    }

    @Override
    public Iterator<T> iterator() {
	LOG.debug(">>> Entering iterator()");
      if (data.isEmpty()) {
        return new InMemoryIterator<>(Collections.emptyIterator());
      }

      KVTypeInfo.Accessor getter = index != null ? ti.getAccessor(index) : null;
	LOG.debug("iterator(): getter → {}", getter);
      int modifier = ascending ? 1 : -1;

      final List<T> sorted = copyElements();
	LOG.debug("iterator(): sorted → {}", sorted);
      sorted.sort((e1, e2) -> modifier * compare(e1, e2, getter));
      Stream<T> stream = sorted.stream();
	LOG.debug("iterator(): stream → {}", stream);

      if (first != null) {
        Comparable<?> firstKey = asKey(first);
        stream = stream.filter(e -> modifier * compare(e, getter, firstKey) >= 0);
      }

      if (last != null) {
        Comparable<?> lastKey = asKey(last);
        stream = stream.filter(e -> modifier * compare(e, getter, lastKey) <= 0);
      }

      if (skip > 0) {
        stream = stream.skip(skip);
      }

      if (max < sorted.size()) {
        stream = stream.limit((int) max);
      }

	LOG.debug("<<< Exiting iterator()");
      return new InMemoryIterator<>(stream.iterator());
    }

    /**
     * Create a copy of the input elements, filtering the values for child indices if needed.
     */
    private List<T> copyElements() {
	LOG.debug(">>> Entering copyElements()");
      if (parent != null) {
        Comparable<Object> parentKey = asKey(parent);
		LOG.debug("copyElements(): parentKey → {}", parentKey);
        if (hasNaturalParentIndex && naturalParentIndexName.equals(ti.getParentIndexName(index))) {
          // If there is a parent index for the natural index and the parent of `index` happens to
          // be it, Spark can use the `parentToChildrenMap` to get the related natural keys, and
          // then copy them from `data`.
          NaturalKeys children = parentToChildrenMap.get(parentKey);
		LOG.debug("copyElements(): children → {}", children);
          if (children == null) {
            return new ArrayList<>();
          }
          ArrayList<T> elements = new ArrayList<>();
		LOG.debug("copyElements(): elements → {}", elements);
          for (Comparable<Object> naturalKey : children.keySet()) {
            data.computeIfPresent(naturalKey, (k, v) -> {
              elements.add(v);
              return v;
            });
          }
          return elements;
        } else {
          // Go through all the values in `data` and collect all the objects has certain parent
          // value. This can be slow when there is a large number of entries in `data`.
          KVTypeInfo.Accessor parentGetter = ti.getParentAccessor(index);
		LOG.debug("copyElements(): parentGetter → {}", parentGetter);
          JavaUtils.checkArgument(parentGetter != null, "Parent filter for non-child index.");
          return data.values().stream()
            .filter(e -> compare(e, parentGetter, parentKey) == 0)
            .collect(Collectors.toList());
        }
      } else {
		LOG.debug("<<< Exiting copyElements()");
		LOG.debug(">>> Entering compare(e1={},e2={},getter={})", e1, e2, getter);
		LOG.error("Exception in compare(e1={},e2={},getter={}): {}", e1, e2, getter, e.getMessage(), e);
		LOG.debug("<<< Exiting compare(e1={},e2={},getter={})", e1, e2, getter);
        return new ArrayList<>(data.values());
      }
    }

    private int compare(T e1, T e2, KVTypeInfo.Accessor getter) {
	LOG.debug(">>> Entering compare(e1={},e2={},getter={})", e1, e2, getter);
      try {
        int diff = compare(e1, getter, asKey(getter.get(e2)));
		LOG.debug("compare(e1={},e2={},getter={}): diff → {}", e1, e2, getter, diff);
        if (diff == 0 && getter != natural) {
          diff = compare(e1, natural, asKey(natural.get(e2)));
        }
        return diff;
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }

    private int compare(T e1, KVTypeInfo.Accessor getter, Comparable<?> v2) {
      try {
        return asKey(getter.get(e1)).compareTo(v2);
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class InMemoryIterator<T> implements KVStoreIterator<T> {

    private final Iterator<T> iter;

    InMemoryIterator(Iterator<T> iter) {
	LOG.debug(">>> Entering InMemoryIterator(iter={})", iter);
      this.iter = iter;
	LOG.debug("<<< Exiting InMemoryIterator(iter={})", iter);
    }

    @Override
    public boolean hasNext() {
	LOG.debug(">>> Entering hasNext()");
	LOG.debug("<<< Exiting hasNext()");
      return iter.hasNext();
    }

    @Override
    public T next() {
	LOG.debug(">>> Entering next()");
	LOG.debug("<<< Exiting next()");
      return iter.next();
    }

    @Override
    public List<T> next(int max) {
      List<T> list = new ArrayList<>(max);
      while (hasNext() && list.size() < max) {
        list.add(next());
      }
      return list;
    }

    @Override
    public boolean skip(long n) {
	LOG.debug(">>> Entering skip(n={})", n);
      long skipped = 0;
      while (skipped < n) {
        if (hasNext()) {
          next();
          skipped++;
        } else {
          return false;
        }
      }

	LOG.debug("<<< Exiting skip(n={})", n);
      return hasNext();
    }

    @Override
    public void close() {
      // no op.
    }

  }

}
