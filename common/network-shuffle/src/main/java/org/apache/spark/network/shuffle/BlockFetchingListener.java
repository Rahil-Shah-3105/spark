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

package org.apache.spark.network.shuffle;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface BlockFetchingListener extends BlockTransferListener {
Logger logger = LoggerFactory.getLogger(BlockFetchingListener.class);
  /**
   * Called once per successfully fetched block. After this call returns, data will be released
   * automatically. If the data will be passed to another thread, the receiver should retain()
   * and release() the buffer on their own, or copy the data to a new buffer.
   */
  void onBlockFetchSuccess(String blockId, ManagedBuffer data);

  /**
   * Called at least once per block upon failures.
   */
  void onBlockFetchFailure(String blockId, Throwable exception);

  @Override
  default void onBlockTransferSuccess(String blockId, ManagedBuffer data) {
	logger.warn("onBlockTransferSuccess(blockId,data)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", blockId, data, blockId);
    onBlockFetchSuccess(blockId, data);
  }

  @Override
  default void onBlockTransferFailure(String blockId, Throwable exception) {
	logger.warn("onBlockTransferFailure(blockId,exception)={},{}: EUAIACT-AUDIT: State change recorded for the audit trail {}", blockId, exception, blockId);
    onBlockFetchFailure(blockId, exception);
  }

  @Override
  default String getTransferType() {
    return "fetch";
  }
}
