/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.data.input.impl;

import com.google.common.base.Preconditions;
import org.apache.commons.io.output.UnsynchronizedByteArrayOutputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class BufferingUtf8InputStreamLineIterator implements Iterator<InputStream>, Closeable
{
  // initial size 8kb, should be enough for a line, but will grow under-the-hood if necessary
  static int BUFFER_INIT_BYTES = 8192;

  // Why not a StringBuilder?
  // Because we focus on copying bytes only, and the StringBuilder requires a decoded
  // stream. Instead, by using bytes only we can copy one at a time without thinking too
  // hard, and then pass the direct values. If a String is required, it can be decoded
  // to a UTF8 String directly.
  static final ThreadLocal<UnsynchronizedByteArrayOutputStream> BUFFER = ThreadLocal.withInitial(() -> new UnsynchronizedByteArrayOutputStream(BUFFER_INIT_BYTES));

  final InputStream source;
  boolean finished;
  InputStream nextLine;

  public BufferingUtf8InputStreamLineIterator(InputStream source)
  {
    Preconditions.checkNotNull(source);
    this.source = source;
    this.finished = false;
    this.nextLine = null;
  }

  @Override
  public void close() throws IOException
  {
    nextLine = null;
    finished = true;
    source.close();
    // Note: do not remove the buffer, as we want to keep it around for subsequent sources
  }

  @Override
  public boolean hasNext()
  {
    //noinspection VariableNotUsedInsideIf
    if (nextLine != null) {
      return true;
    }

    if (finished) {
      return false;
    }

    readNextLine();

    return nextLine != null;
  }

  @Override
  public InputStream next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException("no more lines");
    }

    InputStream result = nextLine;
    nextLine = null;
    return result;
  }

  void readNextLine()
  {
    UnsynchronizedByteArrayOutputStream buff = BUFFER.get();

    try {
      while (true) {
        int b = source.read();

        if (b != -1 && b != '\r' && b != '\n') {
          // ordinary character fastpath
          buff.write(b);

        } else if (b == '\r') {
          // dos uses \r\n as separator
          int nextB = source.read();
          if (nextB == '\n') {
            // a proper \r\n?
            break;
          } else {
            // no - it wasn't a proper \r\n, so append the characters in
            buff.write(b);
            buff.write(nextB);
          }

        } else if (b == '\n') {
          // unix uses \n only
          break;

        } else {
          // must be -1, end of input marker
          finished = true;
          break;
        }
      }

      if (finished && buff.size() == 0) {
        // if there was no last line, do not create an artificial one
        nextLine = null;
      } else {
        nextLine = buff.toInputStream();
      }

      buff.reset();
    }
    catch (IOException e) {
      nextLine = null;
      finished = true;
      throw new IllegalStateException(e);
    }
  }
}
