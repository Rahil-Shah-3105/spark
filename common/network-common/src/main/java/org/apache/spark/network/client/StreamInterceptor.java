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

package org.apache.spark.network.client;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Message;
import org.apache.spark.network.server.MessageHandler;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * An interceptor that is registered with the frame decoder to feed stream data to a
 * callback.
 */
public class StreamInterceptor<T extends Message> implements TransportFrameDecoder.Interceptor {
private static final SparkLogger logger = SparkLoggerFactory.getLogger(StreamInterceptor.class);

  private final MessageHandler<T> handler;
  private final String streamId;
  private final long byteCount;
  private final StreamCallback callback;
  private long bytesRead;

  public StreamInterceptor(
      MessageHandler<T> handler,
      String streamId,
      long byteCount,
      StreamCallback callback) {
    this.handler = handler;
    this.streamId = streamId;
	logger.debug(">>> Entering StreamInterceptor(handler={},streamId={},byteCount={},callback={})", handler, streamId, byteCount, callback);
    this.byteCount = byteCount;
    this.callback = callback;
	logger.debug("StreamInterceptor(handler={},streamId={},byteCount={},callback={}): this.handler → {}", handler, streamId, byteCount, callback, this.handler);
	logger.debug("StreamInterceptor(handler={},streamId={},byteCount={},callback={}): streamId → {}", handler, streamId, byteCount, callback, streamId);
    this.bytesRead = 0;
  }

  @Override
  public void exceptionCaught(Throwable cause) throws Exception {
	logger.debug(">>> Entering exceptionCaught(cause={})", cause);
    deactivateStream();
    callback.onFailure(streamId, cause);
	logger.debug("<<< Exiting exceptionCaught(cause={})", cause);
  }

  @Override
  public void channelInactive() throws Exception {
	logger.debug(">>> Entering channelInactive()");
    deactivateStream();
    callback.onFailure(streamId, new ClosedChannelException());
	logger.debug("<<< Exiting channelInactive()");
  }

  private void deactivateStream() {
	logger.debug(">>> Entering deactivateStream()");
    if (handler instanceof TransportResponseHandler transportResponseHandler) {
      // we only have to do this for TransportResponseHandler as it exposes numOutstandingFetches
      // (there is no extra cleanup that needs to happen)
      transportResponseHandler.deactivateStream();
    }
	logger.debug("<<< Exiting deactivateStream()");
  }

  @Override
  public boolean handle(ByteBuf buf) throws Exception {
	logger.debug(">>> Entering handle(buf={})", buf);
    int toRead = (int) Math.min(buf.readableBytes(), byteCount - bytesRead);
    ByteBuffer nioBuffer = buf.readSlice(toRead).nioBuffer();
	logger.debug("handle(buf={}): nioBuffer → {}", buf, nioBuffer);
	logger.debug("handle(buf={}): toRead → {}", buf, toRead);

    int available = nioBuffer.remaining();
	logger.debug("handle(buf={}): available → {}", buf, available);
    callback.onData(streamId, nioBuffer);
    bytesRead += available;
    if (bytesRead > byteCount) {
      RuntimeException re = new IllegalStateException(String.format(
        "Read too many bytes? Expected %d, but read %d.", byteCount, bytesRead));
      callback.onFailure(streamId, re);
      deactivateStream();
	logger.warn("Throwing re from handle(buf={})", buf);
      throw re;
    } else if (bytesRead == byteCount) {
      deactivateStream();
      callback.onComplete(streamId);
    }

	logger.debug("<<< Exiting handle(buf={})", buf);
    return bytesRead != byteCount;
  }

}
