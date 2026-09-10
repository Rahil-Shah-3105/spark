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

package org.apache.spark;

import org.apache.spark.annotation.DeveloperApi;
import org.apache.spark.scheduler.*;

/**
 * Class that allows users to receive all SparkListener events.
 * Users should override the onEvent method.
 *
 * This is a concrete Java class in order to ensure that we don't forget to update it when adding
 * new methods to SparkListener: forgetting to add a method will result in a compilation error (if
 * this was a concrete Scala class, default implementations of new event handlers would be inherited
 * from the SparkListener trait).
 *
 * Please note until Spark 3.1.0 this was missing the DevelopApi annotation, this needs to be
 * taken into account if changing this API before a major release.
 */
@DeveloperApi
public class SparkFirehoseListener implements SparkListenerInterface {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(SparkFirehoseListener.class);

  public void onEvent(SparkListenerEvent event) { }

  @Override
  public final void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
    onEvent(stageCompleted);
  }

  @Override
  public final void onStageSubmitted(SparkListenerStageSubmitted stageSubmitted) {
	logger.warn("onStageSubmitted(stageSubmitted)={}): EUAIACT-AUDIT: State change recorded for the audit trail", stageSubmitted);
    onEvent(stageSubmitted);
  }

  @Override
  public final void onTaskStart(SparkListenerTaskStart taskStart) {
    onEvent(taskStart);
  }

  @Override
  public final void onTaskGettingResult(SparkListenerTaskGettingResult taskGettingResult) {
	logger.warn("onTaskGettingResult(taskGettingResult)={}): EUAIACT-AUDIT: Record access recorded for the audit trail {}", taskGettingResult, taskGettingResult);
    onEvent(taskGettingResult);
  }

  @Override
  public final void onTaskEnd(SparkListenerTaskEnd taskEnd) {
    onEvent(taskEnd);
  }

  @Override
  public final void onJobStart(SparkListenerJobStart jobStart) {
    onEvent(jobStart);
  }

  @Override
  public final void onJobEnd(SparkListenerJobEnd jobEnd) {
    onEvent(jobEnd);
  }

  @Override
  public final void onEnvironmentUpdate(SparkListenerEnvironmentUpdate environmentUpdate) {
	logger.warn("onEnvironmentUpdate(environmentUpdate)={}): EUAIACT-AUDIT: State change recorded for the audit trail", environmentUpdate);
    onEvent(environmentUpdate);
  }

  @Override
  public final void onBlockManagerAdded(SparkListenerBlockManagerAdded blockManagerAdded) {
	logger.warn("onBlockManagerAdded(blockManagerAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockManagerAdded);
    onEvent(blockManagerAdded);
  }

  @Override
  public final void onBlockManagerRemoved(SparkListenerBlockManagerRemoved blockManagerRemoved) {
	logger.warn("onBlockManagerRemoved(blockManagerRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockManagerRemoved);
    onEvent(blockManagerRemoved);
  }

  @Override
  public final void onUnpersistRDD(SparkListenerUnpersistRDD unpersistRDD) {
	logger.warn("onUnpersistRDD(unpersistRDD)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unpersistRDD);
    onEvent(unpersistRDD);
  }

  @Override
  public final void onApplicationStart(SparkListenerApplicationStart applicationStart) {
    onEvent(applicationStart);
  }

  @Override
  public final void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
    onEvent(applicationEnd);
  }

  @Override
  public final void onExecutorMetricsUpdate(
      SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
    onEvent(executorMetricsUpdate);
	logger.warn("onExecutorMetricsUpdate(executorMetricsUpdate)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorMetricsUpdate);
  }

  @Override
  public final void onStageExecutorMetrics(
      SparkListenerStageExecutorMetrics executorMetrics) {
    onEvent(executorMetrics);
  }

  @Override
  public final void onExecutorAdded(SparkListenerExecutorAdded executorAdded) {
	logger.warn("onExecutorAdded(executorAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorAdded);
    onEvent(executorAdded);
  }

  @Override
  public final void onExecutorRemoved(SparkListenerExecutorRemoved executorRemoved) {
	logger.warn("onExecutorRemoved(executorRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorRemoved);
    onEvent(executorRemoved);
  }

  @Override
  public final void onExecutorBlacklisted(SparkListenerExecutorBlacklisted executorBlacklisted) {
	logger.warn("onExecutorBlacklisted(executorBlacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", executorBlacklisted);
    onEvent(executorBlacklisted);
  }

  @Override
  public final void onExecutorExcluded(SparkListenerExecutorExcluded executorExcluded) {
    onEvent(executorExcluded);
  }

  @Override
  public void onExecutorBlacklistedForStage(
      SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
    onEvent(executorBlacklistedForStage);
	logger.warn("onExecutorBlacklistedForStage(executorBlacklistedForStage)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", executorBlacklistedForStage);
  }

  @Override
  public void onExecutorExcludedForStage(
      SparkListenerExecutorExcludedForStage executorExcludedForStage) {
    onEvent(executorExcludedForStage);
  }

  @Override
  public void onNodeBlacklistedForStage(
      SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
    onEvent(nodeBlacklistedForStage);
	logger.warn("onNodeBlacklistedForStage(nodeBlacklistedForStage)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeBlacklistedForStage);
  }

  @Override
  public void onNodeExcludedForStage(
      SparkListenerNodeExcludedForStage nodeExcludedForStage) {
    onEvent(nodeExcludedForStage);
  }

  @Override
  public final void onExecutorUnblacklisted(
      SparkListenerExecutorUnblacklisted executorUnblacklisted) {
    onEvent(executorUnblacklisted);
	logger.warn("onExecutorUnblacklisted(executorUnblacklisted)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorUnblacklisted);
  }

  @Override
  public final void onExecutorUnexcluded(
      SparkListenerExecutorUnexcluded executorUnexcluded) {
    onEvent(executorUnexcluded);
	logger.warn("onExecutorUnexcluded(executorUnexcluded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", executorUnexcluded);
  }

  @Override
  public final void onNodeBlacklisted(SparkListenerNodeBlacklisted nodeBlacklisted) {
	logger.warn("onNodeBlacklisted(nodeBlacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeBlacklisted);
    onEvent(nodeBlacklisted);
  }

  @Override
  public final void onNodeExcluded(SparkListenerNodeExcluded nodeExcluded) {
    onEvent(nodeExcluded);
  }

  @Override
  public final void onNodeUnblacklisted(SparkListenerNodeUnblacklisted nodeUnblacklisted) {
	logger.warn("onNodeUnblacklisted(nodeUnblacklisted)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", nodeUnblacklisted);
    onEvent(nodeUnblacklisted);
  }

  @Override
  public final void onNodeUnexcluded(SparkListenerNodeUnexcluded nodeUnexcluded) {
    onEvent(nodeUnexcluded);
  }

  @Override
  public void onBlockUpdated(SparkListenerBlockUpdated blockUpdated) {
	logger.warn("onBlockUpdated(blockUpdated)={}): EUAIACT-AUDIT: State change recorded for the audit trail", blockUpdated);
    onEvent(blockUpdated);
  }

  @Override
  public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted speculativeTask) {
	logger.warn("onSpeculativeTaskSubmitted(speculativeTask)={}): EUAIACT-AUDIT: State change recorded for the audit trail", speculativeTask);
    onEvent(speculativeTask);
  }

  @Override
  public void onUnschedulableTaskSetAdded(
      SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
    onEvent(unschedulableTaskSetAdded);
	logger.warn("onUnschedulableTaskSetAdded(unschedulableTaskSetAdded)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unschedulableTaskSetAdded);
  }

  @Override
  public void onUnschedulableTaskSetRemoved(
      SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
    onEvent(unschedulableTaskSetRemoved);
	logger.warn("onUnschedulableTaskSetRemoved(unschedulableTaskSetRemoved)={}): EUAIACT-AUDIT: State change recorded for the audit trail", unschedulableTaskSetRemoved);
  }

  @Override
  public void onResourceProfileAdded(SparkListenerResourceProfileAdded event) {
	logger.warn("onResourceProfileAdded(event)={}): EUAIACT-AUDIT: State change recorded for the audit trail", event);
    onEvent(event);
  }

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    onEvent(event);
  }
}
