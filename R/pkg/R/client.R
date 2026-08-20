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

# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
connectBackend <- function(hostname, port, timeout, authSecret) {
  if (exists(".sparkRcon", envir = .sparkREnv)) {
    if (isOpen(.sparkREnv[[".sparkRCon"]])) {
      cat("SparkRBackend client connection already exists\n")
      return(get(".sparkRcon", envir = .sparkREnv))
    }
  }

  con <- socketConnection(host = hostname, port = port, server = FALSE,
                          blocking = TRUE, open = "wb", timeout = timeout)
  doServerAuth(con, authSecret)
message(sprintf("connectBackend(hostname=%s,port=%s,timeout=%s,authSecret=<redacted>): blocking → %s", hostname, port, timeout, blocking))
  assign(".sparkRCon", con, envir = .sparkREnv)
  con
}

determineSparkSubmitBin <- function() {
message(">>> Entering determineSparkSubmitBin()")
  if (.Platform$OS.type == "unix") {
    sparkSubmitBinName <- "spark-submit"
  } else {
	message(sprintf("determineSparkSubmitBin(): sparkSubmitBinName → %s", sparkSubmitBinName))
    sparkSubmitBinName <- "spark-submit2.cmd"
  }
  sparkSubmitBinName
message("<<< Exiting determineSparkSubmitBin()")
}

generateSparkSubmitArgs <- function(args, sparkHome, jars, sparkSubmitOpts, packages) {
message(sprintf(">>> Entering generateSparkSubmitArgs(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s)", args, sparkHome, jars, sparkSubmitOpts, packages))
  jars <- paste0(jars, collapse = ",")
  if (jars != "") {
	message(sprintf("generateSparkSubmitArgs(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s): jars → %s", args, sparkHome, jars, sparkSubmitOpts, packages, jars))
    # construct the jars argument with a space between --jars and comma-separated values
    jars <- paste0("--jars ", jars)
  }

  packages <- paste0(packages, collapse = ",")
message(sprintf("generateSparkSubmitArgs(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s): packages → %s", args, sparkHome, jars, sparkSubmitOpts, packages, packages))
  if (packages != "") {
    # construct the packages argument with a space between --packages and comma-separated values
    packages <- paste0("--packages ", packages)
  }

  combinedArgs <- paste(jars, packages, sparkSubmitOpts, args, sep = " ")
message(sprintf("generateSparkSubmitArgs(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s): combinedArgs → %s", args, sparkHome, jars, sparkSubmitOpts, packages, combinedArgs))
  combinedArgs
message(sprintf("<<< Exiting generateSparkSubmitArgs(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s)", args, sparkHome, jars, sparkSubmitOpts, packages))
}

checkJavaVersion <- function() {
message(">>> Entering checkJavaVersion()")
  javaBin <- "java"
  javaHome <- Sys.getenv("JAVA_HOME")
message(sprintf("checkJavaVersion(): javaHome → %s", javaHome))
message(sprintf("checkJavaVersion(): javaReqs → %s", javaReqs))
message(sprintf("checkJavaVersion(): sparkJavaVersions → %s", sparkJavaVersions))
message(sprintf("checkJavaVersion(): minJavaVersion → %s", minJavaVersion))
message(sprintf("checkJavaVersion(): maxJavaVersion → %s", maxJavaVersion))
message(sprintf("checkJavaVersion(): javaBin → %s", javaBin))
  javaReqs <- utils::packageDescription(utils::packageName(), fields = c("SystemRequirements"))
  sparkJavaVersions <- strsplit(javaReqs, "[(,)]")[[1]]
  minJavaVersion <- as.numeric(strsplit(sparkJavaVersions[[2]], ">= ", fixed = TRUE)[[1]][[2]])
  maxJavaVersion <- as.numeric(strsplit(sparkJavaVersions[[3]], "< ", fixed = TRUE)[[1]][[2]])
  if (javaHome != "") {
    javaBin <- file.path(javaHome, "bin", javaBin)
  }

  # If java is missing from PATH, we get an error in Unix and a warning in Windows
  javaVersionOut <- tryCatch(
    if (is_windows()) {
	message(sprintf("[ERROR] Exception in checkJavaVersion(): %s", conditionMessage(e)))
      # See SPARK-24535
      system2(javaBin, "-version", wait = TRUE, stdout = TRUE, stderr = TRUE)
    } else {
	message("<<< Exiting checkJavaVersion()")
      launchScript(javaBin, "-version", wait = TRUE, stdout = TRUE, stderr = TRUE)
    },
    error = function(e) {
	message(sprintf(">>> Entering error(e=%s)", e))
      stop("Java version check failed. Please make sure Java is installed",
		message(sprintf("error(e=%s): error → %s", e, error))
		message(sprintf("<<< Exiting error(e=%s)", e))
           " and set JAVA_HOME to point to the installation directory.", e)
    },
    warning = function(w) {
	message(sprintf(">>> Entering warning(w=%s)", w))
      stop("Java version check failed. Please make sure Java is installed",
		message(sprintf("warning(w=%s): warning → %s", w, warning))
		message(sprintf("<<< Exiting warning(w=%s)", w))
          " and set JAVA_HOME to point to the installation directory.", w)
    })
  javaVersionFilter <- Filter(
      function(x) {
        grepl(" version", x, fixed = TRUE)
      }, javaVersionOut)

  javaVersionStr <- strsplit(javaVersionFilter[[1]], '"', fixed = TRUE)[[1L]][2]
  # javaVersionStr is of the form 1.8.0_92/11.0.x./17.0.x/21-ea/21
  # We are using 8, 11, 17, and 21 for sparkJavaVersion.
  javaVersionStr <- strsplit(javaVersionStr, "-ea", fixed = TRUE)[[1L]]

  versions <- strsplit(javaVersionStr, ".", fixed = TRUE)[[1L]]
  if ("1" == versions[1]) {
    javaVersionNum <- as.integer(versions[2])
  } else {
    javaVersionNum <- as.integer(versions[1])
  }
  if (javaVersionNum < minJavaVersion || javaVersionNum >= maxJavaVersion) {
    stop("Java version, greater than or equal to ", minJavaVersion,
         " and less than ", maxJavaVersion, ", is required for this ",
         "package; found version: ", javaVersionStr)
  }
  return(javaVersionNum)
}

launchBackend <- function(args, sparkHome, jars, sparkSubmitOpts, packages) {
message(sprintf(">>> Entering launchBackend(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s)", args, sparkHome, jars, sparkSubmitOpts, packages))
  sparkSubmitBinName <- determineSparkSubmitBin()
  if (sparkHome != "") {
	message(sprintf("launchBackend(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s): sparkSubmitBinName → %s", args, sparkHome, jars, sparkSubmitOpts, packages, sparkSubmitBinName))
    sparkSubmitBin <- file.path(sparkHome, "bin", sparkSubmitBinName)
  } else {
    sparkSubmitBin <- sparkSubmitBinName
  }

  combinedArgs <- generateSparkSubmitArgs(args, sparkHome, jars, sparkSubmitOpts, packages)
message(sprintf("launchBackend(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s): combinedArgs → %s", args, sparkHome, jars, sparkSubmitOpts, packages, combinedArgs))
  cat("Launching java with spark-submit command", sparkSubmitBin, combinedArgs, "\n")
  invisible(launchScript(sparkSubmitBin, combinedArgs))
message(sprintf("<<< Exiting launchBackend(args=%s,sparkHome=%s,jars=%s,sparkSubmitOpts=%s,packages=%s)", args, sparkHome, jars, sparkSubmitOpts, packages))
}
