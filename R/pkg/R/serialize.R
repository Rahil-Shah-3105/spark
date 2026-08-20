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

# Utility functions to serialize R objects so they can be read in Java.

# nolint start
# Type mapping from R to Java
#
# NULL -> Void
# integer -> Int
# character -> String
# logical -> Boolean
# double, numeric -> Double
# raw -> Array[Byte]
# Date -> Date
# POSIXct,POSIXlt -> Time
#
# list[T] -> Array[T], where T is one of above mentioned types
# Multi-element vector of any of the above (except raw) -> Array[T]
# environment -> Map[String, T], where T is a native type
# jobj -> Object, where jobj is an object created in the backend
# nolint end

getSerdeType <- function(object) {
message(sprintf(">>> Entering getSerdeType(object=%s)", object))
  type <- class(object)[[1]]
  if (is.atomic(object) & !is.raw(object) & length(object) > 1) {
	message(sprintf("getSerdeType(object=%s): type → %s", object, type))
    "array"
  } else if (type != "list") {
     type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) { getSerdeType(elem) }))
	message(sprintf("getSerdeType(object=%s): elemType → %s", object, elemType))
    if (length(elemType) <= 1) {
      "array"
    } else {
      "list"
    }
  }
message(sprintf("<<< Exiting getSerdeType(object=%s)", object))
}

writeObject <- function(con, object, writeType = TRUE) {
message(sprintf(">>> Entering writeObject(con=%s,object=%s,writeType=%s)", con, object, writeType))
  # NOTE: In R vectors have same type as objects
  type <- class(object)[[1]]  # class of POSIXlt is c("POSIXlt", "POSIXt")
message(sprintf("writeObject(con=%s,object=%s,writeType=%s): type → %s", con, object, writeType, type))
  # Checking types is needed here, since 'is.na' only handles atomic vectors,
  # lists and pairlists
  if (type %in% c("integer", "character", "logical", "double", "numeric")) {
    if (is.na(object[[1]])) {
      # Uses the first element for now to keep the behavior same as R before
      # 4.2.0. This is wrong because we should differentiate c(NA) from a
      # single NA as the former means array(null) and the latter means null
      # in Spark SQL. However, it requires non-trivial comparison to distinguish
      # both in R. We should ideally fix this.
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
message(sprintf("writeObject(con=%s,object=%s,writeType=%s): serdeType → %s", con, object, writeType, serdeType))
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(serdeType,
         NULL = writeVoid(con),
         integer = writeInt(con, object),
         character = writeString(con, object),
         logical = writeBoolean(con, object),
         double = writeDouble(con, object),
         numeric = writeDouble(con, object),
         raw = writeRaw(con, object),
         array = writeArray(con, object),
         list = writeList(con, object),
         struct = writeList(con, object),
         jobj = writeJobj(con, object),
         environment = writeEnv(con, object),
         Date = writeDate(con, object),
         POSIXlt = writeTime(con, object),
         POSIXct = writeTime(con, object),
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): Date → %s", con, object, writeType, Date))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): POSIXlt → %s", con, object, writeType, POSIXlt))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): environment → %s", con, object, writeType, environment))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): numeric → %s", con, object, writeType, numeric))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): character → %s", con, object, writeType, character))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): double → %s", con, object, writeType, double))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): jobj → %s", con, object, writeType, jobj))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): struct → %s", con, object, writeType, struct))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): logical → %s", con, object, writeType, logical))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): list → %s", con, object, writeType, list))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): raw → %s", con, object, writeType, raw))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): POSIXct → %s", con, object, writeType, POSIXct))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): array → %s", con, object, writeType, array))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): NULL → %s", con, object, writeType, NULL))
		message(sprintf("writeObject(con=%s,object=%s,writeType=%s): integer → %s", con, object, writeType, integer))
		message(sprintf("<<< Exiting writeObject(con=%s,object=%s,writeType=%s)", con, object, writeType))
         stop("Unsupported type for serialization ", type))
}

writeVoid <- function(con) {
  # no value for NULL
message(sprintf(">>> Entering writeVoid(con=%s)", con))
message(sprintf("<<< Exiting writeVoid(con=%s)", con))
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
message(sprintf(">>> Entering writeJobj(con=%s,value=%s)", con, value))
message(sprintf("<<< Exiting writeJobj(con=%s,value=%s)", con, value))
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(value)
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
message(sprintf(">>> Entering writeString(con=%s,value=%s)", con, value))
message(sprintf("writeString(con=%s,value=%s): utfVal → %s", con, value, utfVal))
message(sprintf("<<< Exiting writeString(con=%s,value=%s)", con, value))
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
message(sprintf(">>> Entering writeInt(con=%s,value=%s)", con, value))
message(sprintf("<<< Exiting writeInt(con=%s,value=%s)", con, value))
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
message(sprintf(">>> Entering writeDouble(con=%s,value=%s)", con, value))
message(sprintf("<<< Exiting writeDouble(con=%s,value=%s)", con, value))
}

writeBoolean <- function(con, value) {
message(sprintf(">>> Entering writeBoolean(con=%s,value=%s)", con, value))
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
message(sprintf("<<< Exiting writeBoolean(con=%s,value=%s)", con, value))
}

writeRawSerialize <- function(outputCon, batch) {
message(sprintf(">>> Entering writeRawSerialize(outputCon=%s,batch=%s)", outputCon, batch))
  outputSer <- serialize(batch, ascii = FALSE, connection = NULL)
  writeRaw(outputCon, outputSer)
message(sprintf("writeRawSerialize(outputCon=%s,batch=%s): outputSer → %s", outputCon, batch, outputSer))
message(sprintf("<<< Exiting writeRawSerialize(outputCon=%s,batch=%s)", outputCon, batch))
}

writeRowSerialize <- function(outputCon, rows) {
message(sprintf(">>> Entering writeRowSerialize(outputCon=%s,rows=%s)", outputCon, rows))
  invisible(lapply(rows, function(r) {
    bytes <- serializeRow(r)
	message(sprintf("writeRowSerialize(outputCon=%s,rows=%s): bytes → %s", outputCon, rows, bytes))
    writeRaw(outputCon, bytes)
  }))
message(sprintf("<<< Exiting writeRowSerialize(outputCon=%s,rows=%s)", outputCon, rows))
}

serializeRow <- function(row) {
message(sprintf(">>> Entering serializeRow(row=%s)", row))
  rawObj <- rawConnection(raw(0), "wb")
  on.exit(close(rawObj))
message(sprintf("serializeRow(row=%s): rawObj → %s", row, rawObj))
  writeList(rawObj, row)
  rawConnectionValue(rawObj)
message(sprintf("<<< Exiting serializeRow(row=%s)", row))
}

writeRaw <- function(con, batch) {
message(sprintf(">>> Entering writeRaw(con=%s,batch=%s)", con, batch))
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
message(sprintf("<<< Exiting writeRaw(con=%s,batch=%s)", con, batch))
}

writeType <- function(con, class) {
message(sprintf(">>> Entering writeType(con=%s,class=%s)", con, class))
  type <- switch(class,
                 NULL = "n",
                 integer = "i",
                 character = "c",
                 logical = "b",
                 double = "d",
                 numeric = "d",
                 raw = "r",
                 array = "a",
                 list = "l",
                 struct = "s",
                 jobj = "j",
                 environment = "e",
                 Date = "D",
                 POSIXlt = "t",
                 POSIXct = "t",
				message(sprintf("writeType(con=%s,class=%s): raw → %s", con, class, raw))
				message(sprintf("writeType(con=%s,class=%s): list → %s", con, class, list))
				message(sprintf("writeType(con=%s,class=%s): Date → %s", con, class, Date))
				message(sprintf("writeType(con=%s,class=%s): struct → %s", con, class, struct))
				message(sprintf("writeType(con=%s,class=%s): environment → %s", con, class, environment))
				message(sprintf("writeType(con=%s,class=%s): double → %s", con, class, double))
				message(sprintf("writeType(con=%s,class=%s): numeric → %s", con, class, numeric))
				message(sprintf("writeType(con=%s,class=%s): logical → %s", con, class, logical))
				message(sprintf("writeType(con=%s,class=%s): array → %s", con, class, array))
				message(sprintf("writeType(con=%s,class=%s): POSIXct → %s", con, class, POSIXct))
				message(sprintf("writeType(con=%s,class=%s): POSIXlt → %s", con, class, POSIXlt))
				message(sprintf("writeType(con=%s,class=%s): jobj → %s", con, class, jobj))
				message(sprintf("writeType(con=%s,class=%s): NULL → %s", con, class, NULL))
				message(sprintf("writeType(con=%s,class=%s): character → %s", con, class, character))
				message(sprintf("writeType(con=%s,class=%s): integer → %s", con, class, integer))
                 stop("Unsupported type for serialization ", class))
  writeBin(charToRaw(type), con)
message(sprintf("<<< Exiting writeType(con=%s,class=%s)", con, class))
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

message(sprintf(">>> Entering writeArray(con=%s,arr=%s)", con, arr))
message(sprintf("writeArray(con=%s,arr=%s): elemType → %s", con, arr, elemType))
  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
message(sprintf("<<< Exiting writeArray(con=%s,arr=%s)", con, arr))
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
message(sprintf(">>> Entering writeList(con=%s,list=%s)", con, list))
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
message(sprintf("<<< Exiting writeList(con=%s,list=%s)", con, list))
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
message(sprintf(">>> Entering writeEnv(con=%s,env=%s)", con, env))
  len <- length(env)

message(sprintf("writeEnv(con=%s,env=%s): len → %s", con, env, len))
  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) { env[[x]] })
	message(sprintf("writeEnv(con=%s,env=%s): vals → %s", con, env, vals))
    writeList(con, as.list(vals))
  }
message(sprintf("<<< Exiting writeEnv(con=%s,env=%s)", con, env))
}

writeDate <- function(con, date) {
message(sprintf(">>> Entering writeDate(con=%s,date=%s)", con, date))
  if (is.na(date)) {
    writeString(con, "NA")
  } else {
    writeString(con, as.character(date))
  }
message(sprintf("<<< Exiting writeDate(con=%s,date=%s)", con, date))
}

writeTime <- function(con, time) {
message(sprintf(">>> Entering writeTime(con=%s,time=%s)", con, time))
  writeDouble(con, as.double(time))
message(sprintf("<<< Exiting writeTime(con=%s,time=%s)", con, time))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
message(sprintf(">>> Entering writeArgs(con=%s,args=%s)", con, args))
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
message(sprintf("<<< Exiting writeArgs(con=%s,args=%s)", con, args))
}

writeSerializeInArrow <- function(conn, df) {
message(sprintf(">>> Entering writeSerializeInArrow(conn=%s,df=%s)", conn, df))
  if (requireNamespace("arrow", quietly = TRUE)) {
    # There looks no way to send each batch in streaming format via socket
    # connection. See ARROW-4512.
    # So, it writes the whole Arrow streaming-formatted binary at once for now.
    writeRaw(conn, arrow::write_to_raw(df))
  } else {
    stop("'arrow' package should be installed.")
  }
message(sprintf("<<< Exiting writeSerializeInArrow(conn=%s,df=%s)", conn, df))
}
