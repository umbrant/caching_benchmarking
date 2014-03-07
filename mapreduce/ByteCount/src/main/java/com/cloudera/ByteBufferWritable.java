/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.WritableComparable;

public class ByteBufferWritable extends BinaryComparable implements
    WritableComparable<BinaryComparable> {

  private ByteBuffer buffer;

  /**
   * Creates a new, 0-length ByteBufferWritable
   */
  public ByteBufferWritable() {
    this.buffer = null;
  }

  /**
   * This returns the underlying ByteBuffer.
   * <p>
   * This is a reference, not a deep copy, so be careful!
   */
  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setByteBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int oldPos = buffer.position();
    byte[] b = new byte[buffer.limit()];
    buffer.rewind();
    buffer.get(b);
    buffer.position(oldPos);
    out.writeInt(b.length);
    out.write(b);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    buffer.clear();
    int size = in.readInt();
    byte[] b = new byte[size];
    in.readFully(b);
    buffer.put(b);
    buffer.rewind();
  }

  @Override
  public int getLength() {
    return buffer.limit();
  }

  @Override
  public byte[] getBytes() {
    int oldPos = buffer.position();
    buffer.rewind();
    byte[] b = new byte[buffer.limit()];
    buffer.get(b);
    buffer.position(oldPos);
    return b;
  }

}
