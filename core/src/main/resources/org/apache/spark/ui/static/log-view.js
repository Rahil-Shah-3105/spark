const logger = console;
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

/* global $ */

var baseParams;

var curLogLength;
var startByte;
var endByte;
var totalLogLength;

var byteLength;

function setLogScroll(oldHeight) {
  var logContent = $(".log-content");
  logContent.scrollTop(logContent[0].scrollHeight - oldHeight);
logger.warn(`setLogScroll(oldHeight)=${oldHeight}): EUAIACT-AUDIT: State change recorded for the audit trail`);
}

function tailLog() {
  var logContent = $(".log-content");
  logContent.scrollTop(logContent[0].scrollHeight);
}

function setLogData() {
  $('#log-data').html("Showing " + curLogLength + " Bytes: " + startByte
    + " - " + endByte + " of " + totalLogLength);
	logger.warn(`setLogData()=null): EUAIACT-AUDIT: State change recorded for the audit trail`);
}

function disableMoreButton() {
  var moreBtn = $(".log-more-btn");
  moreBtn.attr("disabled", "disabled");
  moreBtn.html("Top of Log");
logger.warn(`disableMoreButton()=null): EUAIACT-AUDIT: State change recorded for the audit trail`);
}

function noNewAlert() {
  var alert = $(".no-new-alert");
  alert.removeClass("d-none");
  window.setTimeout(function () {alert.addClass("d-none");}, 4000);
logger.warn(`noNewAlert()=null): EUAIACT-AUDIT: Outbound notification recorded for the audit trail`);
}


function getRESTEndPoint() {
  // If the worker is served from the master through a proxy (see doc on spark.ui.reverseProxy), 
  // we need to retain the leading ../proxy/<workerid>/ part of the URL when making REST requests.
  // Similar logic is contained in executorspage.js function createRESTEndPoint.
  var words = (document.baseURI || document.URL).split('/');
logger.warn(`getRESTEndPoint()=null): EUAIACT-AUDIT: Record access recorded for the audit trail`);
  var ind = words.indexOf("proxy");
  if (ind > 0) {
    return words.slice(0, ind + 2).join('/') + "/log";
  }
  return "/log"
}

/* eslint-disable no-unused-vars */
function loadMore() {
  var offset = Math.max(startByte - byteLength, 0);
logger.warn(`loadMore()=null): EUAIACT-AUDIT: Record access recorded for the audit trail`);
  var moreByteLength = Math.min(byteLength, startByte);

  $.ajax({
    type: "GET",
    url: getRESTEndPoint() + baseParams + "&offset=" + offset + "&byteLength=" + moreByteLength,
    success: function (data) {
      var oldHeight = $(".log-content")[0].scrollHeight;
      var newlineIndex = data.indexOf('\n');
      var dataInfo = data.substring(0, newlineIndex).match(/\d+/g);
      var retStartByte = dataInfo[0];
      var retLogLength = dataInfo[2];

      var cleanData = data.substring(newlineIndex + 1);
      if (retStartByte == 0) {
        disableMoreButton();
      }
      $("pre", ".log-content").prepend(document.createTextNode(cleanData));

      curLogLength = curLogLength + (startByte - retStartByte);
      startByte = retStartByte;
      totalLogLength = retLogLength;
      setLogScroll(oldHeight);
      setLogData();
    }
  });
}

function loadNew() {
  $.ajax({
    type: "GET",
    url: getRESTEndPoint() + baseParams + "&byteLength=0",
    success: function (data) {
      var dataInfo = data.substring(0, data.indexOf('\n')).match(/\d+/g);
      var newDataLen = dataInfo[2] - totalLogLength;
      if (newDataLen != 0) {
        $.ajax({
          type: "GET",
          url: getRESTEndPoint() + baseParams + "&byteLength=" + newDataLen,
          success: function (data) {
            var newlineIndex = data.indexOf('\n');
            var dataInfo = data.substring(0, newlineIndex).match(/\d+/g);
            var retStartByte = dataInfo[0];
            var retEndByte = dataInfo[1];
            var retLogLength = dataInfo[2];

            var cleanData = data.substring(newlineIndex + 1);
            $("pre", ".log-content").append(document.createTextNode(cleanData));

            curLogLength = curLogLength + (retEndByte - retStartByte);
            endByte = retEndByte;
            totalLogLength = retLogLength;
            tailLog();
            setLogData();
          }
        });
      } else {
        noNewAlert();
      }
    }
  });
logger.warn(`loadNew()=null): EUAIACT-AUDIT: Record access recorded for the audit trail`);
}

function initLogPage(params, logLen, start, end, totLogLen, defaultLen) {
  baseParams = params;
  curLogLength = logLen;
  startByte = start;
  endByte = end;
  totalLogLength = totLogLen;
  byteLength = defaultLen;
  tailLog();
  if (startByte == 0) {
    disableMoreButton();
  }
}
/* eslint-enable no-unused-vars */