/*
 * Tencent is pleased to support the open source community by making
 * Firestorm-Spark remote shuffle server available. 
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.spark.shuffle.writer;

import com.google.common.collect.Lists;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterBuffer {

  private static final Logger LOG = LoggerFactory.getLogger(WriterBuffer.class);
  private long copyTime = 0;
  private byte[] buffer;
  private int bufferSize;
  private int nextOffset = 0;
  private List<WrappedBuffer> buffers = Lists.newArrayList();
  private int dataLength = 0;
  private int memoryUsed = 0;

  public WriterBuffer(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public void addRecord(byte[] recordBuffer, int length) {
    if (askForMemory(length)) {
      // buffer has data already, add buffer to list
      if (nextOffset > 0) {
        buffers.add(new WrappedBuffer(buffer, nextOffset));
        nextOffset = 0;
      }
      if (length > bufferSize) {
        buffer = new byte[length];
        memoryUsed += length;
      } else {
        buffer = new byte[bufferSize];
        memoryUsed += bufferSize;
      }
    }

    try {
      System.arraycopy(recordBuffer, 0, buffer, nextOffset, length);
    } catch (Exception e) {
      LOG.error("Unexpect exception for System.arraycopy, length[" + length + "], nextOffset["
          + nextOffset + "], bufferSize[" + bufferSize + "]");
      throw e;
    }

    nextOffset += length;
    dataLength += length;
  }

  public boolean askForMemory(long length) {
    return buffer == null || nextOffset + length > bufferSize;
  }

  public byte[] getData() {
    byte[] data = new byte[dataLength];
    int offset = 0;
    long start = System.currentTimeMillis();
    for (WrappedBuffer wrappedBuffer : buffers) {
      System.arraycopy(wrappedBuffer.getBuffer(), 0, data, offset, wrappedBuffer.getSize());
      offset += wrappedBuffer.getSize();
    }
    // nextOffset is the length of current buffer used
    System.arraycopy(buffer, 0, data, offset, nextOffset);
    copyTime += System.currentTimeMillis() - start;
    return data;
  }

  public int getDataLength() {
    return dataLength;
  }

  public long getCopyTime() {
    return copyTime;
  }

  public int getMemoryUsed() {
    return memoryUsed;
  }

  private static final class WrappedBuffer {

    byte[] buffer;
    int size;

    WrappedBuffer(byte[] buffer, int size) {
      this.buffer = buffer;
      this.size = size;
    }

    public byte[] getBuffer() {
      return buffer;
    }

    public int getSize() {
      return size;
    }
  }
}
