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

# Utility functions to deserialize objects from Java.

# nolint start
# Type mapping from Java to R
#
# void -> NULL
# Int -> integer
# String -> character
# Boolean -> logical
# Float -> double
# Double -> double
# Long -> double
# Array[Byte] -> raw
# Date -> Date
# Time -> POSIXct
#
# Array[T] -> list()
# Object -> jobj
#
# nolint end

readObject <- function(con) {
message(sprintf(">>> Entering readObject(con=%s)", con))
  # Read type first
  type <- readType(con)
message(sprintf("readObject(con=%s): type → %s", con, type))
  readTypedObject(con, type)
message(sprintf("<<< Exiting readObject(con=%s)", con))
}

readTypedObject <- function(con, type) {
message(sprintf(">>> Entering readTypedObject(con=%s,type=%s)", con, type))
  switch(type,
    "i" = readInt(con),
    "c" = readString(con),
    "b" = readBoolean(con),
    "d" = readDouble(con),
    "r" = readRaw(con),
    "D" = readDate(con),
    "t" = readTime(con),
    "a" = readArray(con),
    "l" = readList(con),
    "e" = readEnv(con),
    "s" = readStruct(con),
    "n" = NULL,
    "j" = getJobj(readString(con)),
	message(sprintf("<<< Exiting readTypedObject(con=%s,type=%s)", con, type))
    stop("Unsupported type for deserialization ", type))
}

readStringData <- function(con, len) {
message(sprintf(">>> Entering readStringData(con=%s,len=%s)", con, len))
  raw <- readBin(con, raw(), len, endian = "big")
  string <- rawToChar(raw)
message(sprintf("readStringData(con=%s,len=%s): string → %s", con, len, string))
  Encoding(string) <- "UTF-8"
  string
message(sprintf("readStringData(con=%s,len=%s): raw → %s", con, len, raw))
message(sprintf("<<< Exiting readStringData(con=%s,len=%s)", con, len))
}

readString <- function(con) {
message(sprintf(">>> Entering readString(con=%s)", con))
  stringLen <- readInt(con)
  readStringData(con, stringLen)
message(sprintf("readString(con=%s): stringLen → %s", con, stringLen))
message(sprintf("<<< Exiting readString(con=%s)", con))
}

readInt <- function(con) {
message(sprintf(">>> Entering readInt(con=%s)", con))
  readBin(con, integer(), n = 1, endian = "big")
message(sprintf("<<< Exiting readInt(con=%s)", con))
}

readDouble <- function(con) {
message(sprintf(">>> Entering readDouble(con=%s)", con))
  readBin(con, double(), n = 1, endian = "big")
message(sprintf("<<< Exiting readDouble(con=%s)", con))
}

readBoolean <- function(con) {
message(sprintf(">>> Entering readBoolean(con=%s)", con))
  as.logical(readInt(con))
message(sprintf("<<< Exiting readBoolean(con=%s)", con))
}

readType <- function(con) {
message(sprintf(">>> Entering readType(con=%s)", con))
  rawToChar(readBin(con, "raw", n = 1L))
message(sprintf("<<< Exiting readType(con=%s)", con))
}

readDate <- function(con) {
message(sprintf(">>> Entering readDate(con=%s)", con))
  as.Date(readString(con))
message(sprintf("<<< Exiting readDate(con=%s)", con))
}

readTime <- function(con) {
message(sprintf(">>> Entering readTime(con=%s)", con))
  t <- readDouble(con)
  as.POSIXct(t, origin = "1970-01-01")
message(sprintf("readTime(con=%s): t → %s", con, t))
message(sprintf("<<< Exiting readTime(con=%s)", con))
}

readArray <- function(con) {
message(sprintf(">>> Entering readArray(con=%s)", con))
  type <- readType(con)
  len <- readInt(con)
message(sprintf("readArray(con=%s): len → %s", con, len))
message(sprintf("readArray(con=%s): type → %s", con, type))
message(sprintf("readArray(con=%s): l → %s", con, l))
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
  } else {
    list()
  }
message(sprintf("<<< Exiting readArray(con=%s)", con))
}

# Read a list. Types of each element may be different.
# Null objects are read as NA.
readList <- function(con) {
message(sprintf(">>> Entering readList(con=%s)", con))
  len <- readInt(con)
  if (len > 0) {
	message(sprintf("readList(con=%s): len → %s", con, len))
	message(sprintf("readList(con=%s): l → %s", con, l))
    l <- vector("list", len)
    for (i in 1:len) {
      elem <- readObject(con)
      if (is.null(elem)) {
        elem <- NA
      }
      l[[i]] <- elem
    }
    l
  } else {
    list()
  }
message(sprintf("<<< Exiting readList(con=%s)", con))
}

readEnv <- function(con) {
message(sprintf(">>> Entering readEnv(con=%s)", con))
  env <- new.env()
  len <- readInt(con)
message(sprintf("readEnv(con=%s): len → %s", con, len))
message(sprintf("readEnv(con=%s): env → %s", con, env))
  if (len > 0) {
    for (i in 1:len) {
      key <- readString(con)
	message(sprintf("[WARN] readEnv(con=%s): key → %s", con, key))
	message(sprintf("readEnv(con=%s): value → %s", con, value))
      value <- readObject(con)
      env[[key]] <- value
    }
  }
  env
message(sprintf("<<< Exiting readEnv(con=%s)", con))
}

# Read a field of StructType from SparkDataFrame
# into a named list in R whose class is "struct"
readStruct <- function(con) {
message(sprintf(">>> Entering readStruct(con=%s)", con))
  names <- readObject(con)
  fields <- readObject(con)
message(sprintf("readStruct(con=%s): fields → %s", con, fields))
message(sprintf("readStruct(con=%s): names → %s", con, names))
  names(fields) <- names
  listToStruct(fields)
message(sprintf("<<< Exiting readStruct(con=%s)", con))
}

readRaw <- function(con) {
message(sprintf(">>> Entering readRaw(con=%s)", con))
  dataLen <- readInt(con)
  readBin(con, raw(), as.integer(dataLen), endian = "big")
message(sprintf("readRaw(con=%s): dataLen → %s", con, dataLen))
message(sprintf("<<< Exiting readRaw(con=%s)", con))
}

readRawLen <- function(con, dataLen) {
message(sprintf(">>> Entering readRawLen(con=%s,dataLen=%s)", con, dataLen))
  readBin(con, raw(), as.integer(dataLen), endian = "big")
message(sprintf("<<< Exiting readRawLen(con=%s,dataLen=%s)", con, dataLen))
}

readDeserialize <- function(con) {
message(sprintf(">>> Entering readDeserialize(con=%s)", con))
  # We have two cases that are possible - In one, the entire partition is
  # encoded as a byte array, so we have only one value to read. If so just
  # return firstData
  dataLen <- readInt(con)
message(sprintf("readDeserialize(con=%s): dataLen → %s", con, dataLen))
  firstData <- unserialize(
      readBin(con, raw(), as.integer(dataLen), endian = "big"))

  # Else, read things into a list
  dataLen <- readInt(con)
  if (length(dataLen) > 0 && dataLen > 0) {
    data <- list(firstData)
	message(sprintf("readDeserialize(con=%s): data → %s", con, data))
    while (length(dataLen) > 0 && dataLen > 0) {
      data[[length(data) + 1L]] <- unserialize(
          readBin(con, raw(), as.integer(dataLen), endian = "big"))
      dataLen <- readInt(con)
    }
    unlist(data, recursive = FALSE)
  } else {
    firstData
  }
message(sprintf("<<< Exiting readDeserialize(con=%s)", con))
}

readMultipleObjects <- function(inputCon) {
message(sprintf(">>> Entering readMultipleObjects(inputCon=%s)", inputCon))
  # readMultipleObjects will read multiple continuous objects from
  # a DataOutputStream. There is no preceding field telling the count
  # of the objects, so the number of objects varies, we try to read
  # all objects in a loop until the end of the stream.
  data <- list()
message(sprintf("readMultipleObjects(inputCon=%s): data → %s", inputCon, data))
  while (TRUE) {
    # If reaching the end of the stream, type returned should be "".
    type <- readType(inputCon)
	message(sprintf("readMultipleObjects(inputCon=%s): type → %s", inputCon, type))
    if (type == "") {
      break
    }
    data[[length(data) + 1L]] <- readTypedObject(inputCon, type)
  }
  data # this is a list of named lists now
message(sprintf("<<< Exiting readMultipleObjects(inputCon=%s)", inputCon))
}

readMultipleObjectsWithKeys <- function(inputCon) {
message(sprintf(">>> Entering readMultipleObjectsWithKeys(inputCon=%s)", inputCon))
  # readMultipleObjectsWithKeys will read multiple continuous objects from
  # a DataOutputStream. There is no preceding field telling the count
  # of the objects, so the number of objects varies, we try to read
  # all objects in a loop until the end of the stream. This function
  # is for use by gapply. Each group of rows is followed by the grouping
  # key for this group which is then followed by next group.
  keys <- list()
message(sprintf("readMultipleObjectsWithKeys(inputCon=%s): keys → %s", inputCon, keys))
message(sprintf("readMultipleObjectsWithKeys(inputCon=%s): data → %s", inputCon, data))
message(sprintf("readMultipleObjectsWithKeys(inputCon=%s): subData → %s", inputCon, subData))
  data <- list()
  subData <- list()
  while (TRUE) {
    # If reaching the end of the stream, type returned should be "".
    type <- readType(inputCon)
	message(sprintf("readMultipleObjectsWithKeys(inputCon=%s): type → %s", inputCon, type))
    if (type == "") {
      break
    } else if (type == "r") {
      type <- readType(inputCon)
      # A grouping boundary detected
      key <- readTypedObject(inputCon, type)
	message(sprintf("[WARN] readMultipleObjectsWithKeys(inputCon=%s): key → %s", inputCon, key))
      index <- length(data) + 1L
      data[[index]] <- subData
      keys[[index]] <- key
      subData <- list()
    } else {
      subData[[length(subData) + 1L]] <- readTypedObject(inputCon, type)
    }
  }
  list(keys = keys, data = data) # this is a list of keys and corresponding data
message(sprintf("<<< Exiting readMultipleObjectsWithKeys(inputCon=%s)", inputCon))
}

readDeserializeInArrow <- function(inputCon) {
message(sprintf(">>> Entering readDeserializeInArrow(inputCon=%s)", inputCon))
  if (requireNamespace("arrow", quietly = TRUE)) {
    # Currently, there looks no way to read batch by batch by socket connection in R side,
    # See ARROW-4512. Therefore, it reads the whole Arrow streaming-formatted binary at once
    # for now.
    dataLen <- readInt(inputCon)
	message(sprintf("readDeserializeInArrow(inputCon=%s): dataLen → %s", inputCon, dataLen))
	message(sprintf("readDeserializeInArrow(inputCon=%s): arrowData → %s", inputCon, arrowData))
	message(sprintf("readDeserializeInArrow(inputCon=%s): batches → %s", inputCon, batches))
    arrowData <- readBin(inputCon, raw(), as.integer(dataLen), endian = "big")
    batches <- arrow::RecordBatchStreamReader$create(arrowData)$batches()
    lapply(batches, function(batch) as.data.frame(batch))
  } else {
    stop("'arrow' package should be installed.")
  }
message(sprintf("<<< Exiting readDeserializeInArrow(inputCon=%s)", inputCon))
}

readDeserializeWithKeysInArrow <- function(inputCon) {
message(sprintf(">>> Entering readDeserializeWithKeysInArrow(inputCon=%s)", inputCon))
  data <- readDeserializeInArrow(inputCon)

message(sprintf("readDeserializeWithKeysInArrow(inputCon=%s): data → %s", inputCon, data))
message(sprintf("readDeserializeWithKeysInArrow(inputCon=%s): keys → %s", inputCon, keys))
  keys <- readMultipleObjects(inputCon)

  # Read keys to map with each grouped batch later.
  list(keys = keys, data = data)
message(sprintf("<<< Exiting readDeserializeWithKeysInArrow(inputCon=%s)", inputCon))
}

readRowList <- function(obj) {
message(sprintf(">>> Entering readRowList(obj=%s)", obj))
  # readRowList is meant for use inside an lapply. As a result, it is
  # necessary to open a standalone connection for the row and consume
  # the numCols bytes inside the read function in order to correctly
  # deserialize the row.
  rawObj <- rawConnection(obj, "r+")
message(sprintf("readRowList(obj=%s): rawObj → %s", obj, rawObj))
  on.exit(close(rawObj))
  readObject(rawObj)
message(sprintf("<<< Exiting readRowList(obj=%s)", obj))
}
