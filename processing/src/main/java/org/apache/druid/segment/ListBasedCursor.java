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

package org.apache.druid.segment;

import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.BaseQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;

/**
 * A {@link Cursor} that is based on a List of objects. There is no smarts.
 * 
 * Can be used with any list, but best to use one that supports O(1) indexed access
 * such as ArrayList.
 */
public class ListBasedCursor<RowType> implements Cursor
{
  final List<RowType> rows;
  final ColumnSelectorFactory csf;
  final DateTime cursorTime;
  final SettableSupplier<RowType> supplier;

  int position = 0;

  public ListBasedCursor(final List<RowType> rows, final SettableSupplier<RowType> supplier, final ColumnSelectorFactory csf, Granularity gran, Interval interval)
  {
    this.rows = rows;
    this.csf = csf;
    this.cursorTime = gran.toDateTime(interval.getStartMillis());
    this.supplier = supplier;
    setSupplier();
  }

  void setSupplier()
  {
    // TODO, the RowBasedCursor would throw NPE via RowWalker if the target row is null
    // do we need to as well?
    // or even simpler, can we collapse this and use RowBasedCursor

    supplier.set(isDone() ? null : rows.get(position));
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return csf;
  }

  @Override
  public DateTime getTime()
  {
    return cursorTime;
  }

  @Override
  public void advance()
  {
    position += 1;
    setSupplier();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    position += 1;
    setSupplier();
  }

  @Override
  public boolean isDone()
  {
    return position >= rows.size();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    position = 0;
    setSupplier();
  }
}
