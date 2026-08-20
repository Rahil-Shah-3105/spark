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

package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;

/**
 * Request to fetch a sequence of a single chunk of a stream. This will correspond to a single
 * {@link org.apache.spark.network.protocol.ResponseMessage} (either success or failure).
 */
public final class ChunkFetchRequest extends AbstractMessage implements RequestMessage {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(ChunkFetchRequest.class);
  public final StreamChunkId streamChunkId;

  public ChunkFetchRequest(StreamChunkId streamChunkId) {
	logger.debug(">>> Entering ChunkFetchRequest(streamChunkId={})", streamChunkId);
    this.streamChunkId = streamChunkId;
	logger.debug("<<< Exiting ChunkFetchRequest(streamChunkId={})", streamChunkId);
  }

  @Override
  public Message.Type type() { return Type.ChunkFetchRequest; }

  @Override
  public int encodedLength() {
	logger.debug(">>> Entering encodedLength()");
	logger.debug("<<< Exiting encodedLength()");
    return streamChunkId.encodedLength();
  }

  @Override
  public void encode(ByteBuf buf) {
	logger.debug(">>> Entering encode(buf={})", buf);
    streamChunkId.encode(buf);
	logger.debug("<<< Exiting encode(buf={})", buf);
  }

  public static ChunkFetchRequest decode(ByteBuf buf) {
	logger.debug(">>> Entering decode(buf={})", buf);
	logger.debug("<<< Exiting decode(buf={})", buf);
    return new ChunkFetchRequest(StreamChunkId.decode(buf));
  }

  @Override
  public int hashCode() {
	logger.debug(">>> Entering hashCode()");
	logger.debug("<<< Exiting hashCode()");
    return streamChunkId.hashCode();
  }

  @Override
  public boolean equals(Object other) {
	logger.debug(">>> Entering equals(other={})", other);
    if (other instanceof ChunkFetchRequest o) {
	logger.debug("<<< Exiting equals(other={})", other);
      return streamChunkId.equals(o.streamChunkId);
    }
    return false;
  }

  @Override
  public String toString() {
	logger.debug(">>> Entering toString()");
	logger.debug("<<< Exiting toString()");
    return "ChunkFetchRequest[streamChunkId=" + streamChunkId + "]";
  }
}
