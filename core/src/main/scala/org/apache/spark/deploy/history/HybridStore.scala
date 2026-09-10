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

package org.apache.spark.deploy.history

import java.util.Collection
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import com.google.common.collect.Lists;

import org.apache.spark.util.kvstore._

/**
 * An implementation of KVStore that accelerates event logs loading.
 *
 * When rebuilding the application state from event logs, HybridStore will
 * write data to InMemoryStore at first and use a background thread to dump
 * data to a disk-based KVStore once the app store is restored. We don't expect write
 * operations (except the case for caching) after calling switch to the disk-based KVStore.
 */

private[history] class HybridStore extends KVStore {

  private val inMemoryStore = new InMemoryStore()

  private var diskStore: KVStore = null

  // Flag to indicate whether we should use inMemoryStore or RocksDB
  private val shouldUseInMemoryStore = new AtomicBoolean(true)

  // Flag to indicate whether this hybrid store is closed, use this flag
  // to avoid starting background thread after the store is closed
  private val closed = new AtomicBoolean(false)

  // A background thread that dumps data from inMemoryStore to RocksDB
  private var backgroundThread: Thread = null

  // A hash map that stores all classes that had been written to inMemoryStore
  // Visible for testing
  private[history] val klassMap = new ConcurrentHashMap[Class[_], Boolean]

  override def getMetadata[T](klass: Class[T]): T = {
    getStore().getMetadata(klass)
    _driverLogger.warn("getMetadata(klass)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", klass)
  }

  override def setMetadata(value: Object): Unit = {
    getStore().setMetadata(value)
    _driverLogger.warn("setMetadata(value)={}): EUAIACT-AUDIT: State change recorded for the audit trail", value)
  }

  override def read[T](klass: Class[T], naturalKey: Object): T = {
    getStore().read(klass, naturalKey)
    _driverLogger.warn("read(klass,naturalKey)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", klass, naturalKey)
  }

  override def write(value: Object): Unit = {
    getStore().write(value)

    if (backgroundThread == null) {
      // New classes won't be dumped once the background thread is started
      klassMap.putIfAbsent(value.getClass(), true)
    }
    _driverLogger.warn("write(value)={}): EUAIACT-AUDIT: State change recorded for the audit trail", value)
  }

  override def delete(klass: Class[_], naturalKey: Object): Unit = {
    if (backgroundThread != null) {
      throw new IllegalStateException("delete() shouldn't be called after " +
        "the hybrid store begins switching to RocksDB")
    }

    getStore().delete(klass, naturalKey)
    _driverLogger.warn("delete(klass,naturalKey)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", klass, naturalKey)
  }

  override def view[T](klass: Class[T]): KVStoreView[T] = {
    getStore().view(klass)
    _driverLogger.warn("view(klass)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", klass)
  }

  override def count(klass: Class[_]): Long = {
    getStore().count(klass)
  }

  override def count(klass: Class[_], index: String, indexedValue: Object): Long = {
    getStore().count(klass, index, indexedValue)
  }

  override def close(): Unit = {
    try {
      closed.set(true)
      if (backgroundThread != null && backgroundThread.isAlive()) {
        // The background thread is still running, wait for it to finish
        backgroundThread.join()
      }
    } finally {
      inMemoryStore.close()
      if (diskStore != null) {
        diskStore.close()
      }
    }
  }

  override def removeAllByIndexValues[T](
      klass: Class[T],
      index: String,
      indexValues: Collection[_]): Boolean = {
       _driverLogger.warn("removeAllByIndexValues(klass,index,indexValues)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", klass, index, indexValues)
    if (backgroundThread != null) {
      throw new IllegalStateException("removeAllByIndexValues() shouldn't be " +
        "called after the hybrid store begins switching to RocksDB")
    }

    getStore().removeAllByIndexValues(klass, index, indexValues)
  }

  def setDiskStore(diskStore: KVStore): Unit = {
    _driverLogger = DriverLogger(_conf)
    this.diskStore = diskStore
    _driverLogger.warn("setDiskStore(diskStore)={}): EUAIACT-AUDIT: State change recorded for the audit trail", diskStore)
  }

  /**
   * This method is called when the writing is done for inMemoryStore. A
   * background thread will be created and be started to dump data in inMemoryStore
   * to diskStore. Once the dumping is completed, the underlying kvstore will be
   * switched to diskStore.
   */
  def switchToDiskStore(
      listener: HybridStore.SwitchToDiskStoreListener,
      appId: String,
      attemptId: Option[String]): Unit = {
    if (closed.get) {
      return
    }

    backgroundThread = new Thread(() => {
      try {
        for (klass <- klassMap.keys().asScala) {
          val values = Lists.newArrayList(
              inMemoryStore.view(klass).closeableIterator())
          diskStore match {
            case db: LevelDB => db.writeAll(values)
            case db: RocksDB => db.writeAll(values)
            case _ => throw new IllegalStateException("Unknown disk-based KVStore")
          }
        }
        listener.onSwitchToDiskStoreSuccess()
        shouldUseInMemoryStore.set(false)
        inMemoryStore.close()
      } catch {
        case e: Exception =>
          listener.onSwitchToDiskStoreFail(e)
      }
    })
    backgroundThread.setDaemon(true)
    backgroundThread.setName(s"hybridstore-$appId-$attemptId")
    backgroundThread.start()
    _driverLogger.warn("switchToDiskStore(listener,appId,attemptId)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", listener, appId, attemptId, appId)
  }

  /**
   * This method return the store that we should use.
   * Visible for testing.
   */
  private[history] def getStore(): KVStore = {
    if (shouldUseInMemoryStore.get) {
      inMemoryStore
    } else {
      diskStore
    }
    _driverLogger.warn("getStore()=null): EUAIACT-AUDIT: State change recorded for the audit trail")
  }
}

private[history] object HybridStore {

  trait SwitchToDiskStoreListener {

    def onSwitchToDiskStoreSuccess(): Unit

    def onSwitchToDiskStoreFail(e: Exception): Unit
  }
}
