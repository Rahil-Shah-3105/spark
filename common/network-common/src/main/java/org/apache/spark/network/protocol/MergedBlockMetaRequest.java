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
 * Request to find the meta information for the specified merged block. The meta information
 * contains the number of chunks in the merged blocks and the maps ids in each chunk.
 *
 * @since 3.2.0
 */
public class MergedBlockMetaRequest extends AbstractMessage implements RequestMessage {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(MergedBlockMetaRequest.class);
  public final long requestId;
  public final String appId;
  public final int shuffleId;
  public final int shuffleMergeId;
  public final int reduceId;

  public MergedBlockMetaRequest(
      long requestId,
      String appId,
      int shuffleId,
      int shuffleMergeId,
      int reduceId) {
    super(null, false);
    this.requestId = requestId;
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.shuffleMergeId = shuffleMergeId;
    this.reduceId = reduceId;
	logger.debug("MergedBlockMetaRequest(requestId={},appId={},shuffleId={},shuffleMergeId={},reduceId={}): requestId → {}", requestId, appId, shuffleId, shuffleMergeId, reduceId, requestId);
  }

  @Override
  public Type type() {
	logger.debug(">>> Entering type()");
    return Type.MergedBlockMetaRequest;
  }

  @Override
  public int encodedLength() {
	logger.debug(">>> Entering encodedLength()");
	logger.debug("<<< Exiting encodedLength()");
    return 8 + Encoders.Strings.encodedLength(appId) + 4 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
	logger.debug(">>> Entering encode(buf={})", buf);
    buf.writeLong(requestId);
    Encoders.Strings.encode(buf, appId);
    buf.writeInt(shuffleId);
    buf.writeInt(shuffleMergeId);
    buf.writeInt(reduceId);
	logger.debug("<<< Exiting encode(buf={})", buf);
  }

  public static MergedBlockMetaRequest decode(ByteBuf buf) {
	logger.debug(">>> Entering decode(buf={})", buf);
    long requestId = buf.readLong();
	logger.debug("decode(buf={}): requestId → {}", buf, requestId);
    String appId = Encoders.Strings.decode(buf);
	logger.debug("decode(buf={}): appId → {}", buf, appId);
    int shuffleId = buf.readInt();
	logger.debug("decode(buf={}): shuffleId → {}", buf, shuffleId);
    int shuffleMergeId = buf.readInt();
	logger.debug("decode(buf={}): shuffleMergeId → {}", buf, shuffleMergeId);
    int reduceId = buf.readInt();
	logger.debug("decode(buf={}): reduceId → {}", buf, reduceId);
	logger.debug("<<< Exiting decode(buf={})", buf);
    return new MergedBlockMetaRequest(requestId, appId, shuffleId, shuffleMergeId, reduceId);
  }

  @Override
  public int hashCode() {
	logger.debug(">>> Entering hashCode()");
	logger.debug("<<< Exiting hashCode()");
    return Objects.hash(requestId, appId, shuffleId, shuffleMergeId, reduceId);
  }

  @Override
  public boolean equals(Object other) {
	logger.debug(">>> Entering equals(other={})", other);
    if (other instanceof MergedBlockMetaRequest o) {
      return requestId == o.requestId && shuffleId == o.shuffleId &&
        shuffleMergeId == o.shuffleMergeId && reduceId == o.reduceId &&
        Objects.equals(appId, o.appId);
    }
	logger.debug("<<< Exiting equals(other={})", other);
    return false;
  }

  @Override
  public String toString() {
	logger.debug(">>> Entering toString()");
	logger.debug("<<< Exiting toString()");
    return "MergedBlockMetaRequest[requestId=" + requestId + ",appId=" + appId + ",shuffleId=" +
        shuffleId + ",shuffleMergeId=" + shuffleMergeId + ",reduceId=" + reduceId + "]";
  }
}
