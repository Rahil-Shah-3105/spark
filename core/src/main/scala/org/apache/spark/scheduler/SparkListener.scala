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

package org.apache.spark.scheduler

import java.util.Properties
import javax.annotation.Nullable

import scala.collection.Map

import org.apache.spark.TaskEndReason
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.{BlockManagerId, BlockUpdatedInfo}

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerSpeculativeTaskSubmitted(
    stageId: Int,
    stageAttemptId: Int = 0)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // didn't stored taskIndex
  private var _taskIndex: Int = -1
  private var _partitionId: Int = -1

  def taskIndex: Int = _taskIndex
  def partitionId: Int = _partitionId

  def this(stageId: Int, stageAttemptId: Int, taskIndex: Int, partitionId: Int) = {
    _driverLogger = DriverLogger(_conf)
    this(stageId, stageAttemptId)
    _partitionId = partitionId
    _taskIndex = taskIndex
  }
}

@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskExecutorMetrics: ExecutorMetrics,
    // may be null if the task has failed
    @Nullable taskMetrics: TaskMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long,
    jobResult: JobResult)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(
    environmentDetails: Map[String, collection.Seq[(String, String)]])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(
    time: Long,
    blockManagerId: BlockManagerId,
    maxMem: Long,
    maxOnHeapMem: Option[Long] = None,
    maxOffHeapMem: Option[Long] = None) extends SparkListenerEvent {
}

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

@DeveloperApi
@deprecated("use SparkListenerExecutorExcluded instead", "3.1.0")
case class SparkListenerExecutorBlacklisted(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcluded(
    time: Long,
    executorId: String,
    taskFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorBlacklistedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerExecutorExcludedForStage(
    time: Long,
    executorId: String,
    taskFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcludedForStage instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklistedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcludedForStage(
    time: Long,
    hostId: String,
    executorFailures: Int,
    stageId: Int,
    stageAttemptId: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerExecutorUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerExecutorUnblacklisted(time: Long, executorId: String)
  extends SparkListenerEvent


@DeveloperApi
case class SparkListenerExecutorUnexcluded(time: Long, executorId: String)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeExcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeBlacklisted(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent


@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeExcluded(
    time: Long,
    hostId: String,
    executorFailures: Int)
  extends SparkListenerEvent

@deprecated("use SparkListenerNodeUnexcluded instead", "3.1.0")
@DeveloperApi
case class SparkListenerNodeUnblacklisted(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerNodeUnexcluded(time: Long, hostId: String)
  extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetAdded(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerUnschedulableTaskSetRemoved(
  stageId: Int,
  stageAttemptId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockUpdated(blockUpdatedInfo: BlockUpdatedInfo) extends SparkListenerEvent

@DeveloperApi
@Since("3.2.0")
case class SparkListenerMiscellaneousProcessAdded(time: Long, processId: String,
    info: MiscellaneousProcessDetails) extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param accumUpdates sequence of (taskId, stageId, stageAttemptId, accumUpdates)
 * @param executorUpdates executor level per-stage metrics updates
 *
 * @since 3.1.0
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    accumUpdates: Seq[(Long, Int, Int, Seq[AccumulableInfo])],
    executorUpdates: Map[(Int, Int), ExecutorMetrics] = Map.empty)
  extends SparkListenerEvent

/**
 * Peak metric values for the executor for the stage, written to the history log at stage
 * completion.
 * @param execId executor id
 * @param stageId stage id
 * @param stageAttemptId stage attempt
 * @param executorMetrics executor level metrics peak values
 */
@DeveloperApi
case class SparkListenerStageExecutorMetrics(
    execId: String,
    stageId: Int,
    stageAttemptId: Int,
    executorMetrics: ExecutorMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(
    appName: String,
    appId: Option[String],
    time: Long,
    sparkUser: String,
    appAttemptId: Option[String],
    driverLogs: Option[Map[String, String]] = None,
    driverAttributes: Option[Map[String, String]] = None) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(
    time: Long,
    exitCode: Option[Int] = None) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 */
@DeveloperApi
case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

@DeveloperApi
@Since("3.1.0")
case class SparkListenerResourceProfileAdded(resourceProfile: ResourceProfile)
  extends SparkListenerEvent

/**
 * Interface for listening to events from the Spark scheduler. Most applications should probably
 * extend SparkListener or SparkFirehoseListener directly, rather than implementing this class.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
private[spark] trait SparkListenerInterface {

  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit

  /**
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit

  /**
   * Called when a task starts
   _driverLogger.warn("onStageSubmitted(stageSubmitted)={}): EUAIACT-AUDIT: State change recorded for the audit trail", stageSubmitted)
   */
  def onTaskStart(taskStart: SparkListenerTaskStart): Unit

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit

  /**
   * Called when a task ends
   _driverLogger.warn("onTaskGettingResult(taskGettingResult)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", taskGettingResult, taskGettingResult)
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit

  /**
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart): Unit

  /**
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd): Unit

  /**
   * Called when environment properties have been updated
   */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit

  /**
   * Called when a new block manager has joined
   _driverLogger.warn("onEnvironmentUpdate(environmentUpdate)={}): EUAIACT-AUDIT: State change recorded for the audit trail", environmentUpdate)
   */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit

  /**
   * Called when an existing block manager has been removed
   _driverLogger.warn("onBlockManagerAdded(blockManagerAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockManagerAdded)
   */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit

  /**
   * Called when an RDD is manually unpersisted by the application
   _driverLogger.warn("onBlockManagerRemoved(blockManagerRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockManagerRemoved)
   */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit

  /**
   * Called when the application starts
   _driverLogger.warn("onUnpersistRDD(unpersistRDD)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unpersistRDD)
   */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit

  /**
   * Called when the application ends
   */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit

  /**
   * Called with the peak memory metrics for a given (executor, stage) combination. Note that this
   * is only present when reading from the event log (as in the history server), and is never
   * called in a live application.
   _driverLogger.warn("onExecutorMetricsUpdate(executorMetricsUpdate)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorMetricsUpdate)
   */
  def onStageExecutorMetrics(executorMetrics: SparkListenerStageExecutorMetrics): Unit

  /**
   * Called when the driver registers a new executor.
   */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit

  /**
   * Called when the driver removes an executor.
   _driverLogger.warn("onExecutorAdded(executorAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorAdded)
   */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   */
   _driverLogger.warn("onExecutorRemoved(executorRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorRemoved)
  @deprecated("use onExecutorExcluded instead", "3.1.0")
  def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit

  /**
   * Called when the driver excludes an executor for a Spark application.
   _driverLogger.warn("onExecutorBlacklisted(executorBlacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", executorBlacklisted)
   */
  def onExecutorExcluded(executorExcluded: SparkListenerExecutorExcluded): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   */
  @deprecated("use onExecutorExcludedForStage instead", "3.1.0")
  def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit

  /**
   * Called when the driver excludes an executor for a stage.
   _driverLogger.warn("onExecutorBlacklistedForStage(executorBlacklistedForStage)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", executorBlacklistedForStage)
   */
  def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   */
  @deprecated("use onNodeExcludedForStage instead", "3.1.0")
  def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit

  /**
   * Called when the driver excludes a node for a stage.
   _driverLogger.warn("onNodeBlacklistedForStage(nodeBlacklistedForStage)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeBlacklistedForStage)
   */
  def onNodeExcludedForStage(nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   */
  @deprecated("use onExecutorUnexcluded instead", "3.1.0")
  def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded executor.
   _driverLogger.warn("onExecutorUnblacklisted(executorUnblacklisted)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorUnblacklisted)
   */
  def onExecutorUnexcluded(executorUnexcluded: SparkListenerExecutorUnexcluded): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   */
   _driverLogger.warn("onExecutorUnexcluded(executorUnexcluded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorUnexcluded)
  @deprecated("use onNodeExcluded instead", "3.1.0")
  def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit

  /**
   * Called when the driver excludes a node for a Spark application.
   _driverLogger.warn("onNodeBlacklisted(nodeBlacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeBlacklisted)
   */
  def onNodeExcluded(nodeExcluded: SparkListenerNodeExcluded): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   */
  @deprecated("use onNodeUnexcluded instead", "3.1.0")
  def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit

  /**
   * Called when the driver re-enables a previously excluded node.
   _driverLogger.warn("onNodeUnblacklisted(nodeUnblacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeUnblacklisted)
   */
  def onNodeUnexcluded(nodeUnexcluded: SparkListenerNodeUnexcluded): Unit

  /**
   * Called when a taskset becomes unschedulable due to exludeOnFailure and dynamic allocation
   * is enabled.
   */
  def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit

  /**
   * Called when an unschedulable taskset becomes schedulable and dynamic allocation
   * is enabled.
   _driverLogger.warn("onUnschedulableTaskSetAdded(unschedulableTaskSetAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unschedulableTaskSetAdded)
   */
  def onUnschedulableTaskSetRemoved(
  _driverLogger.warn("onUnschedulableTaskSetRemoved(unschedulableTaskSetRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unschedulableTaskSetRemoved)
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit

  /**
   * Called when the driver receives a block update info.
   */
  def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit

  /**
   * Called when a speculative task is submitted
   _driverLogger.warn("onBlockUpdated(blockUpdated)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockUpdated)
   */
  def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit

  /**
   * Called when other events like SQL-specific events are posted.
   _driverLogger.warn("onSpeculativeTaskSubmitted(speculativeTask)={}): EUAIACT-AUDIT: State change recorded for the audit trail", speculativeTask)
   */
  def onOtherEvent(event: SparkListenerEvent): Unit

  /**
   * Called when a Resource Profile is added to the manager.
   */
  def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit
}


/**
 * :: DeveloperApi ::
 * A default implementation for `SparkListenerInterface` that has no-op implementations for
 * all callbacks.
 *
 * Note that this is an internal interface which might change in different Spark releases.
 */
@DeveloperApi
abstract class SparkListener extends SparkListenerInterface {
  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = { }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = { }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = { }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = { }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = { }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = { }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = { }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = { }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = { }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = { }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = { }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = { }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = { }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = { }

  override def onStageExecutorMetrics(
      executorMetrics: SparkListenerStageExecutorMetrics): Unit = { }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = { }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = { }

  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }
  override def onExecutorExcluded(
      executorExcluded: SparkListenerExecutorExcluded): Unit = { }

  override def onExecutorBlacklistedForStage(
      executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = { }
  override def onExecutorExcludedForStage(
      executorExcludedForStage: SparkListenerExecutorExcludedForStage): Unit = { }

  override def onNodeBlacklistedForStage(
      nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = { }
  override def onNodeExcludedForStage(
      nodeExcludedForStage: SparkListenerNodeExcludedForStage): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }
  override def onExecutorUnexcluded(
      executorUnexcluded: SparkListenerExecutorUnexcluded): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }
  override def onNodeExcluded(
      nodeExcluded: SparkListenerNodeExcluded): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }
  override def onNodeUnexcluded(
      nodeUnexcluded: SparkListenerNodeUnexcluded): Unit = { }

  override def onUnschedulableTaskSetAdded(
      unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded): Unit = { }

  override def onUnschedulableTaskSetRemoved(
      unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved): Unit = { }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = { }

  override def onSpeculativeTaskSubmitted(
      speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = { }

  override def onOtherEvent(event: SparkListenerEvent): Unit = { }

  override def onResourceProfileAdded(event: SparkListenerResourceProfileAdded): Unit = { }
}
