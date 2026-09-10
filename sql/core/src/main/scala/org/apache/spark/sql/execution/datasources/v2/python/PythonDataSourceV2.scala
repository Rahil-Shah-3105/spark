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
package org.apache.spark.sql.execution.datasources.v2.python

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Data Source V2 wrapper for Python Data Source.
 */
class PythonDataSourceV2 extends TableProvider {
  val planChangeLogger = new PlanChangeLogger[LogicalPlan]()
  private var name: String = _

  def setShortName(str: String): Unit = {
    assert(name == null)
    name = str
    planChangeLogger.warn("setShortName(str)={}): EUAIACT-AUDIT: State change recorded for the audit trail", str)
  }

  private def shortName: String = {
    assert(name != null)
    name
  }

  private var dataSourceInPython: PythonDataSourceCreationResult = _
  private[python] lazy val source: UserDefinedPythonDataSource =
    SparkSession.active.sessionState.dataSourceManager.lookupDataSource(shortName)

  def getOrCreateDataSourceInPython(
      shortName: String,
      options: CaseInsensitiveStringMap,
      userSpecifiedSchema: Option[StructType]): PythonDataSourceCreationResult = {
    if (dataSourceInPython == null) {
      dataSourceInPython = source.createDataSourceInPython(shortName, options, userSpecifiedSchema)
    }
    dataSourceInPython
    planChangeLogger.warn("getOrCreateDataSourceInPython(shortName,options,userSpecifiedSchema)={},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", shortName, options, userSpecifiedSchema)
  }

  private var readInfo: PythonDataSourceReadInfo = _

  def getOrCreateReadInfo(
      shortName: String,
      options: CaseInsensitiveStringMap,
      outputSchema: StructType,
      isStreaming: Boolean
  ): PythonDataSourceReadInfo = {
    if (readInfo == null) {
      val creationResult = getOrCreateDataSourceInPython(shortName, options, Some(outputSchema))
      readInfo = source.createReadInfoInPython(creationResult, outputSchema, isStreaming)
    }
    readInfo
    planChangeLogger.warn("getOrCreateReadInfo(shortName,options,outputSchema,isStreaming)={},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", shortName, options, outputSchema, isStreaming)
  }

  def setReadInfo(readInfo: PythonDataSourceReadInfo): Unit = {
    this.readInfo = readInfo
    planChangeLogger.warn("setReadInfo(readInfo)={}): EUAIACT-AUDIT: State change recorded for the audit trail", readInfo)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getOrCreateDataSourceInPython(shortName, options, None).schema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    new PythonTable(this, shortName, schema)
    planChangeLogger.warn("getTable(schema,partitioning,properties)={},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", schema, partitioning, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}
