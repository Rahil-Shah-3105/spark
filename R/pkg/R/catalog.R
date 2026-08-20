#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# catalog.R: SparkSession catalog functions

#' Returns the current default catalog
#'
#' Returns the current default catalog.
#'
#' @return name of the current default catalog.
#' @rdname currentCatalog
#' @name currentCatalog
#' @examples
#' \dontrun{
#' sparkR.session()
#' currentCatalog()
#' }
#' @note since 3.4.0
currentCatalog <- function() {
message(">>> Entering currentCatalog()")
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("currentCatalog(): catalog → %s", catalog))
message(sprintf("currentCatalog(): sparkSession → %s", sparkSession))
  callJMethod(catalog, "currentCatalog")
message("<<< Exiting currentCatalog()")
}

#' Sets the current default catalog
#'
#' Sets the current default catalog.
#'
#' @param catalogName name of the catalog
#' @rdname setCurrentCatalog
#' @name setCurrentCatalog
#' @examples
#' \dontrun{
#' sparkR.session()
#' setCurrentCatalog("spark_catalog")
#' }
#' @note since 3.4.0
setCurrentCatalog <- function(catalogName) {
message(sprintf(">>> Entering setCurrentCatalog(catalogName=%s)", catalogName))
  sparkSession <- getSparkSession()
  if (class(catalogName) != "character") {
	message(sprintf("setCurrentCatalog(catalogName=%s): sparkSession → %s", catalogName, sparkSession))
    stop("catalogName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("setCurrentCatalog(catalogName=%s): catalog → %s", catalogName, catalog))
  invisible(handledCallJMethod(catalog, "setCurrentCatalog", catalogName))
message(sprintf("<<< Exiting setCurrentCatalog(catalogName=%s)", catalogName))
}

#' Returns a list of catalog available
#'
#' Returns a list of catalog available.
#'
#' @return a SparkDataFrame of the list of catalog.
#' @rdname listCatalogs
#' @name listCatalogs
#' @examples
#' \dontrun{
#' sparkR.session()
#' listCatalogs()
#' }
#' @note since 3.4.0
listCatalogs <- function() {
message(">>> Entering listCatalogs()")
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("listCatalogs(): catalog → %s", catalog))
message(sprintf("listCatalogs(): sparkSession → %s", sparkSession))
  dataFrame(callJMethod(callJMethod(catalog, "listCatalogs"), "toDF"))
message("<<< Exiting listCatalogs()")
}

#' (Deprecated) Create an external table
#'
#' Creates an external table based on the dataset in a data source,
#' Returns a SparkDataFrame associated with the external table.
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param tableName a name of the table.
#' @param path the path of files to load.
#' @param source the name of external data source.
#' @param schema the schema of the data required for some data sources.
#' @param ... additional argument(s) passed to the method.
#' @return A SparkDataFrame.
#' @rdname createExternalTable-deprecated
#' @seealso \link{createTable}
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createExternalTable("myjson", path="path/to/json", source="json", schema)
#' }
#' @name createExternalTable
#' @note createExternalTable since 1.4.0
createExternalTable <- function(tableName, path = NULL, source = NULL, schema = NULL, ...) {
  .Deprecated("createTable", old = "createExternalTable")
  createTable(tableName, path, source, schema, ...)
message(sprintf("<<< Exiting createExternalTable(tableName=%s,path=%s,source=%s,schema=%s)", tableName, path, source, schema))
}

#' Creates a table based on the dataset in a data source
#'
#' Creates a table based on the dataset in a data source. Returns a SparkDataFrame associated with
#' the table.
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used. When a \code{path} is specified, an external table is
#' created from the data at the given path. Otherwise a managed table is created.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#'                  The table name can be fully qualified with catalog name since 3.4.0.
#' @param path (optional) the path of files to load.
#' @param source (optional) the name of the data source.
#' @param schema (optional) the schema of the data required for some data sources.
#' @param ... additional named parameters as options for the data source.
#' @return A SparkDataFrame.
#' @rdname createTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' df <- createTable("myjson", path="path/to/json", source="json", schema)
#'
#' createTable("spark_catalog.default.people", source = "json", schema = schema)
#' insertInto(df, "people")
#' }
#' @name createTable
#' @note createTable since 2.2.0
createTable <- function(tableName, path = NULL, source = NULL, schema = NULL, ...) {
message(sprintf(">>> Entering createTable(tableName=%s,path=%s,source=%s,schema=%s)", tableName, path, source, schema))
  sparkSession <- getSparkSession()
  options <- varargsToStrEnv(...)
message(sprintf("createTable(tableName=%s,path=%s,source=%s,schema=%s): options → %s", tableName, path, source, schema, options))
message(sprintf("createTable(tableName=%s,path=%s,source=%s,schema=%s): sparkSession → %s", tableName, path, source, schema, sparkSession))
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
	message(sprintf("createTable(tableName=%s,path=%s,source=%s,schema=%s): source → %s", tableName, path, source, schema, source))
    source <- getDefaultSqlSource()
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("createTable(tableName=%s,path=%s,source=%s,schema=%s): catalog → %s", tableName, path, source, schema, catalog))
  if (is.null(schema)) {
	message(sprintf("createTable(tableName=%s,path=%s,source=%s,schema=%s): sdf → %s", tableName, path, source, schema, sdf))
    sdf <- callJMethod(catalog, "createTable", tableName, source, options)
  } else if (class(schema) == "structType") {
    sdf <- callJMethod(catalog, "createTable", tableName, source, schema$jobj, options)
  } else {
    stop("schema must be a structType.")
  }
  dataFrame(sdf)
message(sprintf("<<< Exiting createTable(tableName=%s,path=%s,source=%s,schema=%s)", tableName, path, source, schema))
}

#' Cache Table
#'
#' Caches the specified table in-memory.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#'                  The table name can be fully qualified with catalog name since 3.4.0.
#' @return SparkDataFrame
#' @rdname cacheTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' cacheTable("table")
#' }
#' @name cacheTable
#' @note cacheTable since 1.4.0
cacheTable <- function(tableName) {
message(sprintf(">>> Entering cacheTable(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("cacheTable(tableName=%s): catalog → %s", tableName, catalog))
message(sprintf("cacheTable(tableName=%s): sparkSession → %s", tableName, sparkSession))
  invisible(handledCallJMethod(catalog, "cacheTable", tableName))
message(sprintf("<<< Exiting cacheTable(tableName=%s)", tableName))
}

#' Uncache Table
#'
#' Removes the specified table from the in-memory cache.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#'                  The table name can be fully qualified with catalog name since 3.4.0.
#' @return SparkDataFrame
#' @rdname uncacheTable
#' @examples
#'\dontrun{
#' sparkR.session()
#' path <- "path/to/file.json"
#' df <- read.json(path)
#' createOrReplaceTempView(df, "table")
#' uncacheTable("table")
#' }
#' @name uncacheTable
#' @note uncacheTable since 1.4.0
uncacheTable <- function(tableName) {
message(sprintf(">>> Entering uncacheTable(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("uncacheTable(tableName=%s): catalog → %s", tableName, catalog))
message(sprintf("uncacheTable(tableName=%s): sparkSession → %s", tableName, sparkSession))
  invisible(handledCallJMethod(catalog, "uncacheTable", tableName))
message(sprintf("<<< Exiting uncacheTable(tableName=%s)", tableName))
}

#' Clear Cache
#'
#' Removes all cached tables from the in-memory cache.
#'
#' @rdname clearCache
#' @examples
#' \dontrun{
#' clearCache()
#' }
#' @name clearCache
#' @note clearCache since 1.4.0
clearCache <- function() {
message(">>> Entering clearCache()")
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("clearCache(): catalog → %s", catalog))
message(sprintf("clearCache(): sparkSession → %s", sparkSession))
  invisible(callJMethod(catalog, "clearCache"))
message("<<< Exiting clearCache()")
}

#' (Deprecated) Drop Temporary Table
#'
#' Drops the temporary table with the given table name in the catalog.
#' If the table has been cached/persisted before, it's also unpersisted.
#'
#' @param tableName The name of the SparkSQL table to be dropped.
#' @seealso \link{dropTempView}
#' @rdname dropTempTable-deprecated
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempTable("table")
#' }
#' @name dropTempTable
#' @note dropTempTable since 1.4.0
dropTempTable <- function(tableName) {
  .Deprecated("dropTempView", old = "dropTempTable")
  if (class(tableName) != "character") {
    stop("tableName must be a string.")
  }
  dropTempView(tableName)
message(sprintf("<<< Exiting dropTempTable(tableName=%s)", tableName))
}

#' Drops the temporary view with the given view name in the catalog.
#'
#' Drops the temporary view with the given view name in the catalog.
#' If the view has been cached before, then it will also be uncached.
#'
#' @param viewName the name of the temporary view to be dropped.
#' @return TRUE if the view is dropped successfully, FALSE otherwise.
#' @rdname dropTempView
#' @name dropTempView
#' @examples
#' \dontrun{
#' sparkR.session()
#' df <- read.df(path, "parquet")
#' createOrReplaceTempView(df, "table")
#' dropTempView("table")
#' }
#' @note since 2.0.0
dropTempView <- function(viewName) {
message(sprintf(">>> Entering dropTempView(viewName=%s)", viewName))
  sparkSession <- getSparkSession()
  if (class(viewName) != "character") {
	message(sprintf("dropTempView(viewName=%s): sparkSession → %s", viewName, sparkSession))
    stop("viewName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("dropTempView(viewName=%s): catalog → %s", viewName, catalog))
  callJMethod(catalog, "dropTempView", viewName)
message(sprintf("<<< Exiting dropTempView(viewName=%s)", viewName))
}

#' Tables
#'
#' Returns a SparkDataFrame containing names of tables in the given database.
#'
#' @param databaseName (optional) name of the database
#'                     The database name can be qualified with catalog name since 3.4.0.
#' @return a SparkDataFrame
#' @rdname tables
#' @seealso \link{listTables}
#' @examples
#'\dontrun{
#' sparkR.session()
#' tables("spark_catalog.hive")
#' }
#' @name tables
#' @note tables since 1.4.0
tables <- function(databaseName = NULL) {
message(sprintf(">>> Entering tables(databaseName=%s)", databaseName))
  # rename column to match previous output schema
  withColumnRenamed(listTables(databaseName), "name", "tableName")
message(sprintf("<<< Exiting tables(databaseName=%s)", databaseName))
}

#' Table Names
#'
#' Returns the names of tables in the given database as an array.
#'
#' @param databaseName (optional) name of the database
#'                     The database name can be qualified with catalog name since 3.4.0.
#' @return a list of table names
#' @rdname tableNames
#' @examples
#'\dontrun{
#' sparkR.session()
#' tableNames("spark_catalog.hive")
#' }
#' @name tableNames
#' @note tableNames since 1.4.0
tableNames <- function(databaseName = NULL) {
message(sprintf(">>> Entering tableNames(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  callJStatic("org.apache.spark.sql.api.r.SQLUtils",
              "getTableNames",
              sparkSession,
			message(sprintf("tableNames(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
			message(sprintf("<<< Exiting tableNames(databaseName=%s)", databaseName))
              databaseName)
}

#' Returns the current default database
#'
#' Returns the current default database.
#'
#' @return name of the current default database.
#' @rdname currentDatabase
#' @name currentDatabase
#' @examples
#' \dontrun{
#' sparkR.session()
#' currentDatabase()
#' }
#' @note since 2.2.0
currentDatabase <- function() {
message(">>> Entering currentDatabase()")
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("currentDatabase(): catalog → %s", catalog))
message(sprintf("currentDatabase(): sparkSession → %s", sparkSession))
  callJMethod(catalog, "currentDatabase")
message("<<< Exiting currentDatabase()")
}

#' Sets the current default database
#'
#' Sets the current default database.
#'
#' @param databaseName name of the database
#' @rdname setCurrentDatabase
#' @name setCurrentDatabase
#' @examples
#' \dontrun{
#' sparkR.session()
#' setCurrentDatabase("default")
#' }
#' @note since 2.2.0
setCurrentDatabase <- function(databaseName) {
message(sprintf(">>> Entering setCurrentDatabase(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  if (class(databaseName) != "character") {
	message(sprintf("setCurrentDatabase(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("setCurrentDatabase(databaseName=%s): catalog → %s", databaseName, catalog))
  invisible(handledCallJMethod(catalog, "setCurrentDatabase", databaseName))
message(sprintf("<<< Exiting setCurrentDatabase(databaseName=%s)", databaseName))
}

#' Checks if the database with the specified name exists.
#'
#' Checks if the database with the specified name exists.
#'
#' @param databaseName name of the database, allowed to be qualified with catalog name
#' @rdname databaseExists
#' @name databaseExists
#' @examples
#' \dontrun{
#' sparkR.session()
#' databaseExists("spark_catalog.default")
#' }
#' @note since 3.4.0
databaseExists <- function(databaseName) {
message(sprintf(">>> Entering databaseExists(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  if (class(databaseName) != "character") {
	message(sprintf("databaseExists(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("databaseExists(databaseName=%s): catalog → %s", databaseName, catalog))
  callJMethod(catalog, "databaseExists", databaseName)
message(sprintf("<<< Exiting databaseExists(databaseName=%s)", databaseName))
}

#' Returns a list of databases available
#'
#' Returns a list of databases available.
#'
#' @return a SparkDataFrame of the list of databases.
#' @rdname listDatabases
#' @name listDatabases
#' @examples
#' \dontrun{
#' sparkR.session()
#' listDatabases()
#' }
#' @note since 2.2.0
listDatabases <- function() {
message(">>> Entering listDatabases()")
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("listDatabases(): catalog → %s", catalog))
message(sprintf("listDatabases(): sparkSession → %s", sparkSession))
  dataFrame(callJMethod(callJMethod(catalog, "listDatabases"), "toDF"))
message("<<< Exiting listDatabases()")
}

#' Get the database with the specified name
#'
#' Get the database with the specified name
#'
#' @param databaseName name of the database, allowed to be qualified with catalog name
#' @return A named list.
#' @rdname getDatabase
#' @name getDatabase
#' @examples
#' \dontrun{
#' sparkR.session()
#' db <- getDatabase("default")
#' }
#' @note since 3.4.0
getDatabase <- function(databaseName) {
message(sprintf(">>> Entering getDatabase(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  if (class(databaseName) != "character") {
	message(sprintf("getDatabase(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("getDatabase(databaseName=%s): catalog → %s", databaseName, catalog))
message(sprintf("getDatabase(databaseName=%s): jdb → %s", databaseName, jdb))
  jdb <- handledCallJMethod(catalog, "getDatabase", databaseName)

  ret <- list(name = callJMethod(jdb, "name"))
message(sprintf("getDatabase(databaseName=%s): ret → %s", databaseName, ret))
message(sprintf("getDatabase(databaseName=%s): jcata → %s", databaseName, jcata))
  jcata <- callJMethod(jdb, "catalog")
  if (is.null(jcata)) {
    ret$catalog <- NA
  } else {
    ret$catalog <- jcata
  }

  jdesc <- callJMethod(jdb, "description")
message(sprintf("getDatabase(databaseName=%s): jdesc → %s", databaseName, jdesc))
  if (is.null(jdesc)) {
    ret$description <- NA
  } else {
    ret$description <- jdesc
  }

  ret$locationUri <- callJMethod(jdb, "locationUri")
  ret
message(sprintf("<<< Exiting getDatabase(databaseName=%s)", databaseName))
}

#' Returns a list of tables or views in the specified database
#'
#' Returns a list of tables or views in the specified database.
#' This includes all temporary views.
#'
#' @param databaseName (optional) name of the database
#'                     The database name can be qualified with catalog name since 3.4.0.
#' @return a SparkDataFrame of the list of tables.
#' @rdname listTables
#' @name listTables
#' @seealso \link{tables}
#' @examples
#' \dontrun{
#' sparkR.session()
#' listTables()
#' listTables("spark_catalog.default")
#' }
#' @note since 2.2.0
listTables <- function(databaseName = NULL) {
message(sprintf(">>> Entering listTables(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
	message(sprintf("listTables(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("listTables(databaseName=%s): catalog → %s", databaseName, catalog))
message(sprintf("listTables(databaseName=%s): jdst → %s", databaseName, jdst))
  jdst <- if (is.null(databaseName)) {
    callJMethod(catalog, "listTables")
  } else {
    handledCallJMethod(catalog, "listTables", databaseName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
message(sprintf("<<< Exiting listTables(databaseName=%s)", databaseName))
}

#' Checks if the table with the specified name exists.
#'
#' Checks if the table with the specified name exists.
#'
#' @param tableName name of the table, allowed to be qualified with catalog name
#' @rdname tableExists
#' @name tableExists
#' @examples
#' \dontrun{
#' sparkR.session()
#' databaseExists("spark_catalog.default.myTable")
#' }
#' @note since 3.4.0
tableExists <- function(tableName) {
message(sprintf(">>> Entering tableExists(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  if (class(tableName) != "character") {
	message(sprintf("tableExists(tableName=%s): sparkSession → %s", tableName, sparkSession))
    stop("tableName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("tableExists(tableName=%s): catalog → %s", tableName, catalog))
  callJMethod(catalog, "tableExists", tableName)
message(sprintf("<<< Exiting tableExists(tableName=%s)", tableName))
}

#' Get the table with the specified name
#'
#' Get the table with the specified name
#'
#' @param tableName the qualified or unqualified name that designates a table, allowed to be
#'                  qualified with catalog name
#' @return A named list.
#' @rdname getTable
#' @name getTable
#' @examples
#' \dontrun{
#' sparkR.session()
#' tbl <- getTable("spark_catalog.default.myTable")
#' }
#' @note since 3.4.0
getTable <- function(tableName) {
message(sprintf(">>> Entering getTable(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  if (class(tableName) != "character") {
	message(sprintf("getTable(tableName=%s): sparkSession → %s", tableName, sparkSession))
    stop("tableName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("getTable(tableName=%s): catalog → %s", tableName, catalog))
message(sprintf("getTable(tableName=%s): jtbl → %s", tableName, jtbl))
  jtbl <- handledCallJMethod(catalog, "getTable", tableName)

  ret <- list(name = callJMethod(jtbl, "name"))
message(sprintf("getTable(tableName=%s): ret → %s", tableName, ret))
message(sprintf("getTable(tableName=%s): jcata → %s", tableName, jcata))
  jcata <- callJMethod(jtbl, "catalog")
  if (is.null(jcata)) {
    ret$catalog <- NA
  } else {
    ret$catalog <- jcata
  }

  jns <- callJMethod(jtbl, "namespace")
message(sprintf("getTable(tableName=%s): jns → %s", tableName, jns))
  if (is.null(jns)) {
    ret$namespace <- NA
  } else {
    ret$namespace <- jns
  }

  jdesc <- callJMethod(jtbl, "description")
message(sprintf("getTable(tableName=%s): jdesc → %s", tableName, jdesc))
  if (is.null(jdesc)) {
    ret$description <- NA
  } else {
    ret$description <- jdesc
  }

  ret$tableType <- callJMethod(jtbl, "tableType")
  ret$isTemporary <- callJMethod(jtbl, "isTemporary")
  ret
message(sprintf("<<< Exiting getTable(tableName=%s)", tableName))
}

#' Returns a list of columns for the given table/view in the specified database
#'
#' Returns a list of columns for the given table/view in the specified database.
#'
#' @param tableName the qualified or unqualified name that designates a table/view. If no database
#'                  identifier is provided, it refers to a table/view in the current database.
#'                  If \code{databaseName} parameter is specified, this must be an unqualified name.
#'                  The table name can be qualified with catalog name since 3.4.0, when databaseName
#'                  is NULL.
#' @param databaseName (optional) name of the database
#' @return a SparkDataFrame of the list of column descriptions.
#' @rdname listColumns
#' @name listColumns
#' @examples
#' \dontrun{
#' sparkR.session()
#' listColumns("spark_catalog.default.mytable")
#' }
#' @note since 2.2.0
listColumns <- function(tableName, databaseName = NULL) {
message(sprintf(">>> Entering listColumns(tableName=%s,databaseName=%s)", tableName, databaseName))
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
	message(sprintf("listColumns(tableName=%s,databaseName=%s): sparkSession → %s", tableName, databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("listColumns(tableName=%s,databaseName=%s): catalog → %s", tableName, databaseName, catalog))
message(sprintf("listColumns(tableName=%s,databaseName=%s): jdst → %s", tableName, databaseName, jdst))
  jdst <- if (is.null(databaseName)) {
    handledCallJMethod(catalog, "listColumns", tableName)
  } else {
    handledCallJMethod(catalog, "listColumns", databaseName, tableName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
message(sprintf("<<< Exiting listColumns(tableName=%s,databaseName=%s)", tableName, databaseName))
}

#' Returns a list of functions registered in the specified database
#'
#' Returns a list of functions registered in the specified database.
#' This includes all temporary functions.
#'
#' @param databaseName (optional) name of the database
#'                     The database name can be qualified with catalog name since 3.4.0.
#' @return a SparkDataFrame of the list of function descriptions.
#' @rdname listFunctions
#' @name listFunctions
#' @examples
#' \dontrun{
#' sparkR.session()
#' listFunctions(spark_catalog.default)
#' }
#' @note since 2.2.0
listFunctions <- function(databaseName = NULL) {
message(sprintf(">>> Entering listFunctions(databaseName=%s)", databaseName))
  sparkSession <- getSparkSession()
  if (!is.null(databaseName) && class(databaseName) != "character") {
	message(sprintf("listFunctions(databaseName=%s): sparkSession → %s", databaseName, sparkSession))
    stop("databaseName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("listFunctions(databaseName=%s): catalog → %s", databaseName, catalog))
message(sprintf("listFunctions(databaseName=%s): jdst → %s", databaseName, jdst))
  jdst <- if (is.null(databaseName)) {
    callJMethod(catalog, "listFunctions")
  } else {
    handledCallJMethod(catalog, "listFunctions", databaseName)
  }
  dataFrame(callJMethod(jdst, "toDF"))
message(sprintf("<<< Exiting listFunctions(databaseName=%s)", databaseName))
}

#' Checks if the function with the specified name exists.
#'
#' Checks if the function with the specified name exists.
#'
#' @param functionName name of the function, allowed to be qualified with catalog name
#' @rdname functionExists
#' @name functionExists
#' @examples
#' \dontrun{
#' sparkR.session()
#' functionExists("spark_catalog.default.myFunc")
#' }
#' @note since 3.4.0
functionExists <- function(functionName) {
message(sprintf(">>> Entering functionExists(functionName=%s)", functionName))
  sparkSession <- getSparkSession()
  if (class(functionName) != "character") {
	message(sprintf("functionExists(functionName=%s): sparkSession → %s", functionName, sparkSession))
    stop("functionName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("functionExists(functionName=%s): catalog → %s", functionName, catalog))
  callJMethod(catalog, "functionExists", functionName)
message(sprintf("<<< Exiting functionExists(functionName=%s)", functionName))
}

#' Get the function with the specified name
#'
#' Get the function with the specified name
#'
#' @param functionName name of the function, allowed to be qualified with catalog name
#' @return A named list.
#' @rdname getFunc
#' @name getFunc
#' @examples
#' \dontrun{
#' sparkR.session()
#' func <- getFunc("spark_catalog.default.myFunc")
#' }
#' @note since 3.4.0. Use different name with the scala/python side, to avoid the
#'       signature conflict with built-in "getFunction".
getFunc <- function(functionName) {
message(sprintf(">>> Entering getFunc(functionName=%s)", functionName))
  sparkSession <- getSparkSession()
  if (class(functionName) != "character") {
	message(sprintf("getFunc(functionName=%s): sparkSession → %s", functionName, sparkSession))
    stop("functionName must be a string.")
  }
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("getFunc(functionName=%s): catalog → %s", functionName, catalog))
message(sprintf("getFunc(functionName=%s): jfunc → %s", functionName, jfunc))
  jfunc <- handledCallJMethod(catalog, "getFunction", functionName)

  ret <- list(name = callJMethod(jfunc, "name"))
message(sprintf("getFunc(functionName=%s): ret → %s", functionName, ret))
message(sprintf("getFunc(functionName=%s): jcata → %s", functionName, jcata))
  jcata <- callJMethod(jfunc, "catalog")
  if (is.null(jcata)) {
    ret$catalog <- NA
  } else {
    ret$catalog <- jcata
  }

  jns <- callJMethod(jfunc, "namespace")
message(sprintf("getFunc(functionName=%s): jns → %s", functionName, jns))
  if (is.null(jns)) {
    ret$namespace <- NA
  } else {
    ret$namespace <- jns
  }

  jdesc <- callJMethod(jfunc, "description")
message(sprintf("getFunc(functionName=%s): jdesc → %s", functionName, jdesc))
  if (is.null(jdesc)) {
    ret$description <- NA
  } else {
    ret$description <- jdesc
  }

  ret$className <- callJMethod(jfunc, "className")
  ret$isTemporary <- callJMethod(jfunc, "isTemporary")
  ret
message(sprintf("<<< Exiting getFunc(functionName=%s)", functionName))
}

#' Recovers all the partitions in the directory of a table and update the catalog
#'
#' Recovers all the partitions in the directory of a table and update the catalog. The name should
#' reference a partitioned table, and not a view.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#'                  The table name can be fully qualified with catalog name since 3.4.0.
#' @rdname recoverPartitions
#' @name recoverPartitions
#' @examples
#' \dontrun{
#' sparkR.session()
#' recoverPartitions("spark_catalog.default.myTable")
#' }
#' @note since 2.2.0
recoverPartitions <- function(tableName) {
message(sprintf(">>> Entering recoverPartitions(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("recoverPartitions(tableName=%s): catalog → %s", tableName, catalog))
message(sprintf("recoverPartitions(tableName=%s): sparkSession → %s", tableName, sparkSession))
  invisible(handledCallJMethod(catalog, "recoverPartitions", tableName))
message(sprintf("<<< Exiting recoverPartitions(tableName=%s)", tableName))
}

#' Invalidates and refreshes all the cached data and metadata of the given table
#'
#' Invalidates and refreshes all the cached data and metadata of the given table. For performance
#' reasons, Spark SQL or the external data source library it uses might cache certain metadata about
#' a table, such as the location of blocks. When those change outside of Spark SQL, users should
#' call this function to invalidate the cache.
#'
#' If this table is cached as an InMemoryRelation, drop the original cached version and make the
#' new version cached lazily.
#'
#' @param tableName the qualified or unqualified name that designates a table. If no database
#'                  identifier is provided, it refers to a table in the current database.
#'                  The table name can be fully qualified with catalog name since 3.4.0.
#' @rdname refreshTable
#' @name refreshTable
#' @examples
#' \dontrun{
#' sparkR.session()
#' refreshTable("spark_catalog.default.myTable")
#' }
#' @note since 2.2.0
refreshTable <- function(tableName) {
message(sprintf(">>> Entering refreshTable(tableName=%s)", tableName))
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("refreshTable(tableName=%s): catalog → %s", tableName, catalog))
message(sprintf("refreshTable(tableName=%s): sparkSession → %s", tableName, sparkSession))
  invisible(handledCallJMethod(catalog, "refreshTable", tableName))
message(sprintf("<<< Exiting refreshTable(tableName=%s)", tableName))
}

#' Invalidates and refreshes all the cached data and metadata for SparkDataFrame containing path
#'
#' Invalidates and refreshes all the cached data (and the associated metadata) for any
#' SparkDataFrame that contains the given data source path. Path matching is by prefix, i.e. "/"
#' would invalidate everything that is cached.
#'
#' @param path the path of the data source.
#' @rdname refreshByPath
#' @name refreshByPath
#' @examples
#' \dontrun{
#' sparkR.session()
#' refreshByPath("/path")
#' }
#' @note since 2.2.0
refreshByPath <- function(path) {
message(sprintf(">>> Entering refreshByPath(path=%s)", path))
  sparkSession <- getSparkSession()
  catalog <- callJMethod(sparkSession, "catalog")
message(sprintf("refreshByPath(path=%s): catalog → %s", path, catalog))
message(sprintf("refreshByPath(path=%s): sparkSession → %s", path, sparkSession))
  invisible(handledCallJMethod(catalog, "refreshByPath", path))
message(sprintf("<<< Exiting refreshByPath(path=%s)", path))
}
