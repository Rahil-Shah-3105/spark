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

import java.util.Objects;

import io.netty.buffer.ByteBuf;

/**
 * Response to {@link ChunkFetchRequest} when there is an error fetching the chunk.
 */
public final class ChunkFetchFailure extends AbstractMessage implements ResponseMessage {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(ChunkFetchFailure.class);
  public final StreamChunkId streamChunkId;
  public final String errorString;

  public ChunkFetchFailure(StreamChunkId streamChunkId, String errorString) {
	logger.debug(">>> Entering ChunkFetchFailure(streamChunkId={},errorString={})", streamChunkId, errorString);
    this.streamChunkId = streamChunkId;
	logger.debug("ChunkFetchFailure(streamChunkId={},errorString={}): this.streamChunkId → {}", streamChunkId, errorString, this.streamChunkId);
    this.errorString = errorString;
	logger.debug("<<< Exiting ChunkFetchFailure(streamChunkId={},errorString={})", streamChunkId, errorString);
  }

  @Override
  public Message.Type type() { return Type.ChunkFetchFailure; }

  @Override
  public int encodedLength() {
	logger.debug(">>> Entering encodedLength()");
	logger.debug("<<< Exiting encodedLength()");
    return streamChunkId.encodedLength() + Encoders.Strings.encodedLength(errorString);
  }

  @Override
  public void encode(ByteBuf buf) {
	logger.debug(">>> Entering encode(buf={})", buf);
    streamChunkId.encode(buf);
    Encoders.Strings.encode(buf, errorString);
	logger.debug("<<< Exiting encode(buf={})", buf);
  }

  public static ChunkFetchFailure decode(ByteBuf buf) {
	logger.debug(">>> Entering decode(buf={})", buf);
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
	logger.debug("decode(buf={}): streamChunkId → {}", buf, streamChunkId);
    String errorString = Encoders.Strings.decode(buf);
	logger.debug("decode(buf={}): errorString → {}", buf, errorString);
	logger.debug("<<< Exiting decode(buf={})", buf);
    return new ChunkFetchFailure(streamChunkId, errorString);
  }

  @Override
  public int hashCode() {
	logger.debug(">>> Entering hashCode()");
	logger.debug("<<< Exiting hashCode()");
    return Objects.hash(streamChunkId, errorString);
  }

  @Override
  public boolean equals(Object other) {
	logger.debug(">>> Entering equals(other={})", other);
    if (other instanceof ChunkFetchFailure o) {
	logger.debug("<<< Exiting equals(other={})", other);
      return streamChunkId.equals(o.streamChunkId) && errorString.equals(o.errorString);
    }
    return false;
  }

  @Override
  public String toString() {
	logger.debug(">>> Entering toString()");
	logger.debug("<<< Exiting toString()");
    return "ChunkFetchFailure[streamChunkId=" + streamChunkId + ",errorString=" + errorString + "]";
  }
}
