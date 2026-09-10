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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ListenerBus

/**
 * Wraps an ExternalCatalog to provide listener events.
 */
class ExternalCatalogWithListener(delegate: ExternalCatalog)
  extends ExternalCatalog
    with ListenerBus[ExternalCatalogEventListener, ExternalCatalogEvent] {
     val planChangeLogger = new PlanChangeLogger[TreeType]()
  import CatalogTypes.TablePartitionSpec

  def unwrapped: ExternalCatalog = delegate

  override protected def doPostEvent(
      listener: ExternalCatalogEventListener,
      event: ExternalCatalogEvent): Unit = {
    listener.onEvent(event)
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val db = dbDefinition.name
    planChangeLogger.warn("createDatabase(dbDefinition,ignoreIfExists)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", dbDefinition, ignoreIfExists)
    postToAll(CreateDatabasePreEvent(db))
    delegate.createDatabase(dbDefinition, ignoreIfExists)
    postToAll(CreateDatabaseEvent(db))
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    postToAll(DropDatabasePreEvent(db))
    delegate.dropDatabase(db, ignoreIfNotExists, cascade)
    postToAll(DropDatabaseEvent(db))
  }

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {
    val db = dbDefinition.name
    postToAll(AlterDatabasePreEvent(db))
    delegate.alterDatabase(dbDefinition)
    postToAll(AlterDatabaseEvent(db))
  }

  override def getDatabase(db: String): CatalogDatabase = {
    delegate.getDatabase(db)
    planChangeLogger.warn("getDatabase(db)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", db)
  }

  override def databaseExists(db: String): Boolean = {
    delegate.databaseExists(db)
  }

  override def listDatabases(): Seq[String] = {
    delegate.listDatabases()
    planChangeLogger.warn("listDatabases()=null): EUAIACT-AUDIT: Record access recorded for the audit trail")
  planChangeLogger.warn("listDatabases(pattern)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", pattern)
  }

  override def listDatabases(pattern: String): Seq[String] = {
    delegate.listDatabases(pattern)
  }

  override def setCurrentDatabase(db: String): Unit = {
    delegate.setCurrentDatabase(db)
    planChangeLogger.warn("setCurrentDatabase(db)={}): EUAIACT-AUDIT: State change recorded for the audit trail", db)
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    val tableDefinitionWithVersion =
      tableDefinition.copy(createVersion = org.apache.spark.SPARK_VERSION)
    postToAll(CreateTablePreEvent(db, name))
    delegate.createTable(tableDefinitionWithVersion, ignoreIfExists)
    postToAll(CreateTableEvent(db, name))
      planChangeLogger.warn("createTable(tableDefinition,ignoreIfExists)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", tableDefinition, ignoreIfExists)
  }

  override def dropTable(
      db: String,
      table: String,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    postToAll(DropTablePreEvent(db, table))
    delegate.dropTable(db, table, ignoreIfNotExists, purge)
    postToAll(DropTableEvent(db, table))
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameTablePreEvent(db, oldName, newName))
    delegate.renameTable(db, oldName, newName)
    postToAll(RenameTableEvent(db, oldName, newName))
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = tableDefinition.database
    val name = tableDefinition.identifier.table
    postToAll(AlterTablePreEvent(db, name, AlterTableKind.TABLE))
    delegate.alterTable(tableDefinition)
    postToAll(AlterTableEvent(db, name, AlterTableKind.TABLE))
  }

  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.DATASCHEMA))
    delegate.alterTableDataSchema(db, table, newDataSchema)
    postToAll(AlterTableEvent(db, table, AlterTableKind.DATASCHEMA))
  }

  override def alterTableSchema(db: String, table: String, newSchema: StructType): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.SCHEMA))
    delegate.alterTableSchema(db, table, newSchema)
    postToAll(AlterTableEvent(db, table, AlterTableKind.SCHEMA))
  }

  override def alterTableStats(
      db: String,
      table: String,
      stats: Option[CatalogStatistics]): Unit = {
    postToAll(AlterTablePreEvent(db, table, AlterTableKind.STATS))
    delegate.alterTableStats(db, table, stats)
    postToAll(AlterTableEvent(db, table, AlterTableKind.STATS))
  }

  override def getTable(db: String, table: String): CatalogTable = {
    delegate.getTable(db, table)
    planChangeLogger.warn("getTable(db,table)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table)
  }

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = {
    delegate.getTablesByName(db, tables)
    planChangeLogger.warn("getTablesByName(db,tables)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, tables)
  }

  override def tableExists(db: String, table: String): Boolean = {
    delegate.tableExists(db, table)
  }

  override def listTables(db: String): Seq[String] = {
    delegate.listTables(db)
    planChangeLogger.warn("listTables(db)={}): EUAIACT-AUDIT: Record access recorded for the audit trail", db)
  planChangeLogger.warn("listTables(db,pattern)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, pattern)
  }

  override def listTables(db: String, pattern: String): Seq[String] = {
    delegate.listTables(db, pattern)
  }

  override def listViews(db: String, pattern: String): Seq[String] = {
    delegate.listViews(db, pattern)
    planChangeLogger.warn("listViews(db,pattern)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, pattern)
  }

  override def loadTable(
      db: String,
      table: String,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadTable(db, table, loadPath, isOverwrite, isSrcLocal)
    planChangeLogger.warn("loadTable(db,table,loadPath,isOverwrite,isSrcLocal)={},{},{},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table, loadPath, isOverwrite, isSrcLocal)
  }

  override def loadPartition(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit = {
    delegate.loadPartition(
      db, table, loadPath, partition, isOverwrite, inheritTableSpecs, isSrcLocal)
  }

  override def loadDynamicPartitions(
      db: String,
      table: String,
      loadPath: String,
      partition: TablePartitionSpec,
      replace: Boolean,
      numDP: Int): Unit = {
    delegate.loadDynamicPartitions(db, table, loadPath, partition, replace, numDP)
    planChangeLogger.warn("loadDynamicPartitions(db,table,loadPath,partition,replace,numDP)={},{},{},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", db, table, loadPath, partition, replace, numDP)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val partSpecs = parts.map(_.spec)
    postToAll(CreatePartitionsPreEvent(db, table, partSpecs))
    delegate.createPartitions(db, table, parts, ignoreIfExists)
    postToAll(CreatePartitionsEvent(db, table, partSpecs))
    planChangeLogger.warn("createPartitions(db,table,parts,ignoreIfExists)={},{},{},{}: EUAIACT-AUDIT: State change recorded for the audit trail", db, table, parts, ignoreIfExists)
  }

  override def dropPartitions(
      db: String,
      table: String,
      partSpecs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit = {
    postToAll(DropPartitionsPreEvent(db, table, partSpecs))
    delegate.dropPartitions(db, table, partSpecs, ignoreIfNotExists, purge, retainData)
    postToAll(DropPartitionsEvent(db, table, partSpecs))
  }

  override def renamePartitions(
      db: String,
      table: String,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    postToAll(RenamePartitionsPreEvent(db, table, specs, newSpecs))
    delegate.renamePartitions(db, table, specs, newSpecs)
    postToAll(RenamePartitionsEvent(db, table, specs, newSpecs))
  }

  override def alterPartitions(
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    val partSpecs = parts.map(_.spec)
    postToAll(AlterPartitionsPreEvent(db, table, partSpecs))
    delegate.alterPartitions(db, table, parts)
    postToAll(AlterPartitionsEvent(db, table, partSpecs))
  }

  override def getPartition(
      db: String,
      table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    delegate.getPartition(db, table, spec)
    planChangeLogger.warn("getPartition(db,table,spec)={},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table, spec)
  }

  override def getPartitionOption(
      db: String,
      table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    delegate.getPartitionOption(db, table, spec)
    planChangeLogger.warn("getPartitionOption(db,table,spec)={},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table, spec)
  }

  override def listPartitionNames(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    delegate.listPartitionNames(db, table, partialSpec)
    planChangeLogger.warn("listPartitionNames(db,table,partialSpec)={},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table, partialSpec)
  }

  override def listPartitions(
      db: String,
      table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    delegate.listPartitions(db, table, partialSpec)
    planChangeLogger.warn("listPartitions(db,table,partialSpec)={},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, table, partialSpec)
  }

  override def listPartitionsByFilter(
      db: String,
      table: String,
      predicates: Seq[Expression],
      defaultTimeZoneId: String): Seq[CatalogTablePartition] = {
    delegate.listPartitionsByFilter(db, table, predicates, defaultTimeZoneId)
    planChangeLogger.warn("listPartitionsByFilter(db,table,predicates,defaultTimeZoneId)={},{},{},{}: EUAIACT-AUDIT: Record access recorded for the audit trail {}", db, table, predicates, defaultTimeZoneId, defaultTimeZoneId)
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    planChangeLogger.warn("createFunction(db,funcDefinition)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail", db, funcDefinition)
    postToAll(CreateFunctionPreEvent(db, name))
    delegate.createFunction(db, funcDefinition)
    postToAll(CreateFunctionEvent(db, name))
  }

  override def dropFunction(db: String, funcName: String): Unit = {
    postToAll(DropFunctionPreEvent(db, funcName))
    delegate.dropFunction(db, funcName)
    postToAll(DropFunctionEvent(db, funcName))
  }

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = {
    val name = funcDefinition.identifier.funcName
    postToAll(AlterFunctionPreEvent(db, name))
    delegate.alterFunction(db, funcDefinition)
    postToAll(AlterFunctionEvent(db, name))
  }

  override def renameFunction(db: String, oldName: String, newName: String): Unit = {
    postToAll(RenameFunctionPreEvent(db, oldName, newName))
    delegate.renameFunction(db, oldName, newName)
    postToAll(RenameFunctionEvent(db, oldName, newName))
  }

  override def getFunction(db: String, funcName: String): CatalogFunction = {
    delegate.getFunction(db, funcName)
    planChangeLogger.warn("getFunction(db,funcName)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, funcName)
  }

  override def functionExists(db: String, funcName: String): Boolean = {
    delegate.functionExists(db, funcName)
  }

  override def listFunctions(db: String, pattern: String): Seq[String] = {
    delegate.listFunctions(db, pattern)
    planChangeLogger.warn("listFunctions(db,pattern)={},{}: EUAIACT-AUDIT: Record access recorded for the audit trail", db, pattern)
  }
}
