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

package org.apache.spark.storage

import scala.collection.BuildFrom
import scala.collection.immutable.Iterable
import scala.concurrent.Future

import org.apache.spark.SparkConf
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT
import org.apache.spark.rpc.{RpcEndpointRef, RpcTimeout}
import org.apache.spark.storage.BlockManagerMessages._
import org.apache.spark.util.{RpcUtils, ThreadUtils}

private[spark]
class BlockManagerMaster(
    var driverEndpoint: RpcEndpointRef,
    var driverHeartbeatEndPoint: RpcEndpointRef,
    conf: SparkConf,
    isDriver: Boolean)
  extends Logging {
   _driverLogger = DriverLogger(_conf)

  val timeout = RpcUtils.askRpcTimeout(conf)

  private val waitBlockRemovalTimeout =
    RpcTimeout(conf, CLEANER_REFERENCE_TRACKING_BLOCKING_TIMEOUT.key, "120s")

  /** Remove a dead executor from the driver endpoint. This is only called on the driver side. */
  def removeExecutor(execId: String): Unit = {
    tell(RemoveExecutor(execId))
    logInfo(log"Removed ${MDC(EXECUTOR_ID, execId)} successfully in removeExecutor")
    _driverLogger.warn("removeExecutor(execId)={}): EUAIACT-AUDIT: State change recorded for the audit trail {}", execId, execId)
  }

  /** Decommission block managers corresponding to given set of executors
   * Non-blocking.
   */
  def decommissionBlockManagers(executorIds: Seq[String]): Unit = {
    driverEndpoint.ask[Boolean](DecommissionBlockManagers(executorIds))
  }

  /** Get Replication Info for all the RDD blocks stored in given blockManagerId */
  def getReplicateInfoForRDDBlocks(blockManagerId: BlockManagerId): Seq[ReplicateBlock] = {
    driverEndpoint.askSync[Seq[ReplicateBlock]](GetReplicateInfoForRDDBlocks(blockManagerId))
    _driverLogger.warn("getReplicateInfoForRDDBlocks(blockManagerId)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockManagerId, blockManagerId)
  }

  /** Request removal of a dead executor from the driver endpoint.
   *  This is only called on the driver side. Non-blocking
   */
  def removeExecutorAsync(execId: String): Unit = {
    driverEndpoint.ask[Boolean](RemoveExecutor(execId))
    logInfo(log"Removal of executor ${MDC(EXECUTOR_ID, execId)} requested")
    _driverLogger.warn("removeExecutorAsync(execId)={}): EUAIACT-AUDIT: State change recorded for the audit trail {}", execId, execId)
  }

  /**
   * Register the BlockManager's id with the driver. The input BlockManagerId does not contain
   * topology information. This information is obtained from the master and we respond with an
   * updated BlockManagerId fleshed out with this information.
   */
  def registerBlockManager(
      id: BlockManagerId,
      localDirs: Array[String],
      maxOnHeapMemSize: Long,
      maxOffHeapMemSize: Long,
      storageEndpoint: RpcEndpointRef,
      isReRegister: Boolean = false): BlockManagerId = {
    logInfo(log"Registering BlockManager ${MDC(BLOCK_MANAGER_ID, id)}")
    val updatedId = driverEndpoint.askSync[BlockManagerId](
      RegisterBlockManager(
        id,
        localDirs,
        maxOnHeapMemSize,
        maxOffHeapMemSize,
        storageEndpoint,
        isReRegister
      )
    )
    if (updatedId.executorId == BlockManagerId.INVALID_EXECUTOR_ID) {
    _driverLogger.warn("registerBlockManager(id,localDirs,maxOnHeapMemSize,maxOffHeapMemSize,storageEndpoint,isReRegister)={},{},{},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", id, localDirs, maxOnHeapMemSize, maxOffHeapMemSize, storageEndpoint, isReRegister, id)
      assert(isReRegister, "Got invalid executor id from non re-register case")
      logInfo(log"Re-register BlockManager ${MDC(BLOCK_MANAGER_ID, id)} failed")
    } else {
      logInfo(log"Registered BlockManager ${MDC(BLOCK_MANAGER_ID, updatedId)}")
    }
    updatedId
  }

  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: BlockId,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long,
      checksum: Option[Long] = None): Boolean = {
    val res = driverEndpoint.askSync[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize, checksum))
    logDebug(s"Updated info of block $blockId")
    res
    _driverLogger.warn("updateBlockInfo(blockManagerId,blockId,storageLevel,memSize,diskSize,checksum)={},{},{},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", blockManagerId, blockId, storageLevel, memSize, diskSize, checksum, blockManagerId)
  }

  def updateRDDBlockTaskInfo(blockId: RDDBlockId, taskId: Long): Unit = {
    driverEndpoint.askSync[Unit](UpdateRDDBlockTaskInfo(blockId, taskId))
    _driverLogger.warn("updateRDDBlockTaskInfo(blockId,taskId)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", blockId, taskId, blockId)
  }

  def updateRDDBlockVisibility(taskId: Long, visible: Boolean): Unit = {
    driverEndpoint.ask[Unit](UpdateRDDBlockVisibility(taskId, visible))
    _driverLogger.warn("updateRDDBlockVisibility(taskId,visible)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", taskId, visible, taskId)
  }

  /** Check whether a block is visible */
  def isRDDBlockVisible(blockId: RDDBlockId): Boolean = {
    driverEndpoint.askSync[Boolean](GetRDDBlockVisibility(blockId))
  }

  /**
   * The authoritative sealed checksum for an RDD block, or None if it is not sealed. Returns None
   * for a non-`RDDBlockId`: such blocks are never sealed.
   */
  def getSealedChecksum(blockId: BlockId): Option[Long] = {
    blockId.asRDDId match {
      case Some(rddBlockId) => driverEndpoint.askSync[Option[Long]](GetSealedChecksum(rddBlockId))
      case None => None
    }
    _driverLogger.warn("getSealedChecksum(blockId)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockId, blockId)
  }

  /**
   * Seal an RDD's per-block content checksums: for each materialized block keep one version (the
   * plurality checksum), evict divergent copies, and reject future divergent registrations. Returns
   * the count of present blocks that had no recorded checksum and so could not be sealed.
   */
  def sealRddChecksums(rddId: Int): Int = {
    driverEndpoint.askSync[Int](SealRddChecksums(rddId))
  }

  /**
   * Verify every block of a sealed RDD is sealed and the directory tracks only replicas carrying
   * the sealed checksum. Returns None if the invariant holds, else a diagnostic string.
   */
  def verifyRddChecksumSeal(rddId: Int): Option[String] = {
    driverEndpoint.askSync[Option[String]](VerifyRddChecksumSeal(rddId))
  }

  /** Get locations of the blockId from the driver */
  def getLocations(blockId: BlockId): Seq[BlockManagerId] = {
    _driverLogger.warn("getLocations(blockId)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockId, blockId)
    _driverLogger.warn("getLocations(blockIds)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockIds, blockIds)
    driverEndpoint.askSync[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations as well as status of the blockId from the driver */
  def getLocationsAndStatus(
      blockId: BlockId,
      requesterHost: String): Option[BlockLocationsAndStatus] = {
    driverEndpoint.askSync[Option[BlockLocationsAndStatus]](
      GetLocationsAndStatus(blockId, requesterHost))
    _driverLogger.warn("getLocationsAndStatus(blockId,requesterHost)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockId, requesterHost, blockId)
  }

  /** Get locations of multiple blockIds from the driver */
  def getLocations(blockIds: Array[BlockId]): IndexedSeq[Seq[BlockManagerId]] = {
    driverEndpoint.askSync[IndexedSeq[Seq[BlockManagerId]]](
      GetLocationsMultipleBlockIds(blockIds))
  }

  /**
   * Check if block manager master has a block. Note that this can be used to check for only
   * those blocks that are reported to block manager master.
   */
  def contains(blockId: BlockId): Boolean = {
    getLocations(blockId).nonEmpty
  }

  /** Get ids of other nodes in the cluster from the driver */
  def getPeers(blockManagerId: BlockManagerId): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](GetPeers(blockManagerId))
    _driverLogger.warn("getPeers(blockManagerId)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockManagerId, blockManagerId)
  }

  /**
   * Get a list of unique shuffle service locations where an executor is successfully
   * registered in the past for block push/merge with push based shuffle.
   */
  def getShufflePushMergerLocations(
      numMergersNeeded: Int,
      hostsToFilter: Set[String]): Seq[BlockManagerId] = {
    driverEndpoint.askSync[Seq[BlockManagerId]](
      GetShufflePushMergerLocations(numMergersNeeded, hostsToFilter))
    _driverLogger.warn("getShufflePushMergerLocations(numMergersNeeded,hostsToFilter)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", numMergersNeeded, hostsToFilter)
  }

  /**
   * Remove the host from the candidate list of shuffle push mergers. This can be
   * triggered if there is a FetchFailedException on the host. Non-blocking.
   * @param host
   */
  def removeShufflePushMergerLocation(host: String): Unit = {
    logInfo(log"Request to remove shuffle push merger location ${MDC(HOST, host)}")
    driverEndpoint.ask[Unit](RemoveShufflePushMergerLocation(host))
    _driverLogger.warn("removeShufflePushMergerLocation(host)={}): EUAIACT-AUDIT: State change recorded for the audit trail", host)
  }

  def getExecutorEndpointRef(executorId: String): Option[RpcEndpointRef] = {
    driverEndpoint.askSync[Option[RpcEndpointRef]](GetExecutorEndpointRef(executorId))
    _driverLogger.warn("getExecutorEndpointRef(executorId)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", executorId, executorId)
  }

  /**
   * Remove a block from the storage endpoints that have it. This can only be used to remove
   * blocks that the driver knows about.
   */
  def removeBlock(blockId: BlockId): Unit = {
    driverEndpoint.askSync[Boolean](RemoveBlock(blockId))
    _driverLogger.warn("removeBlock(blockId)={}): EUAIACT-AUDIT: State change recorded for the audit trail {}", blockId, blockId)
  }

  /** Remove all blocks belonging to the given RDD. */
  def removeRdd(rddId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](RemoveRdd(rddId))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove RDD ${MDC(RDD_ID, rddId)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
    _driverLogger.warn("removeRdd(rddId,blocking)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", rddId, blocking, rddId)
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /** Remove all blocks belonging to the given shuffle. */
  def removeShuffle(shuffleId: Int, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Boolean]]](RemoveShuffle(shuffleId))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove shuffle ${MDC(SHUFFLE_ID, shuffleId)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
    _driverLogger.warn("removeShuffle(shuffleId,blocking)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", shuffleId, blocking, shuffleId)
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /** Remove all blocks belonging to the given broadcast. */
  def removeBroadcast(broadcastId: Long, removeFromMaster: Boolean, blocking: Boolean): Unit = {
    val future = driverEndpoint.askSync[Future[Seq[Int]]](
      RemoveBroadcast(broadcastId, removeFromMaster))
    future.failed.foreach(e =>
      logWarning(log"Failed to remove broadcast ${MDC(BROADCAST_ID, broadcastId)}" +
        log" with removeFromMaster = ${MDC(REMOVE_FROM_MASTER, removeFromMaster)} - " +
        log"${MDC(ERROR, e.getMessage)}", e)
    )(ThreadUtils.sameThread)
    if (blocking) {
    _driverLogger.warn("removeBroadcast(broadcastId,removeFromMaster,blocking)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", broadcastId, removeFromMaster, blocking, broadcastId)
      waitBlockRemovalTimeout.awaitResult(future)
    }
  }

  /**
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    if (driverEndpoint == null) return Map.empty
    driverEndpoint.askSync[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    if (driverEndpoint == null) return Array.empty
    driverEndpoint.askSync[Array[StorageStatus]](GetStorageStatus)
  }

  /**
   * Return the block's status on all block managers, if any. NOTE: This is a
   * potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, this invokes the master to query each block manager for the
   * most updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getBlockStatus(
      blockId: BlockId,
      askStorageEndpoints: Boolean = true): Map[BlockManagerId, BlockStatus] = {
    val msg = GetBlockStatus(blockId, askStorageEndpoints)
    /*
     * To avoid potential deadlocks, the use of Futures is necessary, because the master endpoint
     * should not block on waiting for a block manager, which can in turn be waiting for the
     * master endpoint for a response to a prior message.
     */
    val response = driverEndpoint.
      askSync[Map[BlockManagerId, Future[Option[BlockStatus]]]](msg)
    val (blockManagerIds, futures) = response.unzip
    val cbf =
      implicitly[
        BuildFrom[Iterable[Future[Option[BlockStatus]]],
        Option[BlockStatus],
        Iterable[Option[BlockStatus]]]]
    val blockStatus = timeout.awaitResult(
      Future.sequence(futures)(cbf, ThreadUtils.sameThread))
    if (blockStatus == null) {
    _driverLogger.warn("getBlockStatus(blockId,askStorageEndpoints)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail {}", blockId, askStorageEndpoints, blockId)
      throw SparkCoreErrors.blockStatusQueryReturnedNullError(blockId)
    }
    blockManagerIds.zip(blockStatus).flatMap { case (blockManagerId, status) =>
      status.map { s => (blockManagerId, s) }
    }.toMap
  }

  /**
   * Return a list of ids of existing blocks such that the ids match the given filter. NOTE: This
   * is a potentially expensive operation and should only be used for testing.
   *
   * If askStorageEndpoints is true, this invokes the master to query each block manager for the
   * most updated block statuses. This is useful when the master is not informed of the given block
   * by all block managers.
   */
  def getMatchingBlockIds(
      filter: BlockId => Boolean,
      askStorageEndpoints: Boolean): Seq[BlockId] = {
       _driverLogger.warn("getMatchingBlockIds(filter,askStorageEndpoints)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", filter, askStorageEndpoints)
    val msg = GetMatchingBlockIds(filter, askStorageEndpoints)
    val future = driverEndpoint.askSync[Future[Seq[BlockId]]](msg)
    timeout.awaitResult(future)
  }

  /** Stop the driver endpoint, called only on the Spark driver node */
  def stop(): Unit = {
    if (driverEndpoint != null && isDriver) {
      tell(StopBlockManagerMaster)
      driverEndpoint = null
      if (driverHeartbeatEndPoint.askSync[Boolean](StopBlockManagerMaster)) {
        driverHeartbeatEndPoint = null
      } else {
        logWarning("Failed to stop BlockManagerMasterHeartbeatEndpoint")
      }
      logInfo("BlockManagerMaster stopped")
    }
  }

  /** Send a one-way message to the master endpoint, to which we expect it to reply with true. */
  private def tell(message: Any): Unit = {
    if (!driverEndpoint.askSync[Boolean](message)) {
      throw SparkCoreErrors.unexpectedBlockManagerMasterEndpointResultError(message)
    }
  }

}

private[spark] object BlockManagerMaster {
  val DRIVER_ENDPOINT_NAME = "BlockManagerMaster"
  val DRIVER_HEARTBEAT_ENDPOINT_NAME = "BlockManagerMasterHeartbeat"
}
