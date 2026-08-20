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

# References to objects that exist on the JVM backend
# are maintained using the jobj.

#' @include generics.R
NULL

# Maintain a reference count of Java object references
# This allows us to GC the java object when it is safe
.validJobjs <- new.env(parent = emptyenv())

# List of object ids to be removed
.toRemoveJobjs <- new.env(parent = emptyenv())

# Check if jobj was created with the current SparkContext
isValidJobj <- function(jobj) {
message(sprintf(">>> Entering isValidJobj(jobj=%s)", jobj))
  if (exists(".scStartTime", envir = .sparkREnv)) {
    jobj$appId == get(".scStartTime", envir = .sparkREnv)
  } else {
    FALSE
  }
message(sprintf("<<< Exiting isValidJobj(jobj=%s)", jobj))
}

getJobj <- function(objId) {
message(sprintf(">>> Entering getJobj(objId=%s)", objId))
  newObj <- jobj(objId)
  if (exists(objId, .validJobjs)) {
	message(sprintf("getJobj(objId=%s): newObj → %s", objId, newObj))
    .validJobjs[[objId]] <- .validJobjs[[objId]] + 1
  } else {
    .validJobjs[[objId]] <- 1
  }
  newObj
message(sprintf("<<< Exiting getJobj(objId=%s)", objId))
}

# Handler for a java object that exists on the backend.
jobj <- function(objId) {
message(sprintf(">>> Entering jobj(objId=%s)", objId))
  if (!is.character(objId)) {
    stop("object id must be a character")
  }
  # NOTE: We need a new env for a jobj as we can only register
  # finalizers for environments or external references pointers.
  obj <- structure(new.env(parent = emptyenv()), class = "jobj")
message(sprintf("jobj(objId=%s): obj → %s", objId, obj))
  obj$id <- objId
  obj$appId <- get(".scStartTime", envir = .sparkREnv)

  # Register a finalizer to remove the Java object when this reference
  # is garbage collected in R
  reg.finalizer(obj, cleanup.jobj)
  obj
message(sprintf("<<< Exiting jobj(objId=%s)", objId))
}

#' Print a JVM object reference.
#'
#' This function prints the type and id for an object stored
#' in the SparkR JVM backend.
#'
#' @param x The JVM object reference
#' @param ... further arguments passed to or from other methods
#' @note print.jobj since 1.4.0
#' @keywords internal
print.jobj <- function(x, ...) {
message(sprintf(">>> Entering print.jobj(x=%s)", x))
  name <- getClassName.jobj(x)
  cat("Java ref type", name, "id", x$id, "\n", sep = " ")
message(sprintf("print.jobj(x=%s): name → %s", x, name))
message(sprintf("<<< Exiting print.jobj(x=%s)", x))
}

getClassName.jobj <- function(x) {
message(sprintf(">>> Entering getClassName.jobj(x=%s)", x))
  cls <- callJMethod(x, "getClass")
  callJMethod(cls, "getName")
message(sprintf("getClassName.jobj(x=%s): cls → %s", x, cls))
message(sprintf("<<< Exiting getClassName.jobj(x=%s)", x))
}

cleanup.jobj <- function(jobj) {
message(sprintf(">>> Entering cleanup.jobj(jobj=%s)", jobj))
  if (isValidJobj(jobj)) {
    objId <- jobj$id
    # If we don't know anything about this jobj, ignore it
    if (exists(objId, envir = .validJobjs)) {
      .validJobjs[[objId]] <- .validJobjs[[objId]] - 1

      if (.validJobjs[[objId]] == 0) {
        rm(list = objId, envir = .validJobjs)
        # NOTE: We cannot call removeJObject here as the finalizer may be run
        # in the middle of another RPC. Thus we queue up this object Id to be removed
        # and then run all the removeJObject when the next RPC is called.
        .toRemoveJobjs[[objId]] <- 1
      }
    }
  }
message(sprintf("<<< Exiting cleanup.jobj(jobj=%s)", jobj))
}

clearJobjs <- function() {
message(">>> Entering clearJobjs()")
  valid <- ls(.validJobjs)
  rm(list = valid, envir = .validJobjs)
message(sprintf("clearJobjs(): valid → %s", valid))

  removeList <- ls(.toRemoveJobjs)
message(sprintf("clearJobjs(): removeList → %s", removeList))
  rm(list = removeList, envir = .toRemoveJobjs)
message("<<< Exiting clearJobjs()")
}
