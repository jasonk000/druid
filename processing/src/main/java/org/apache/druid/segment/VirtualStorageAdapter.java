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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A {@link StorageAdapter} that is based on a stream of objects. Generally created by a {@link RowBasedSegment}.
 *
 * @see RowBasedSegment#RowBasedSegment for implementation notes
 */
public class VirtualStorageAdapter implements StorageAdapter, SortedCursorFactory
{
  private final SortedCursorFactory cursorFactory;
  private final RowSignature rowSignature;

  VirtualStorageAdapter(
      final SortedCursorFactory cursorFactory,
      final RowSignature rowSignature
  )
  {
    this.cursorFactory = Preconditions.checkNotNull(cursorFactory, "cursorFactory");
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature");
  }

  @Override
  public Interval getInterval()
  {
    return Intervals.ETERNITY;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(new ArrayList<>(rowSignature.getColumnNames()));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.emptyList();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return Integer.MAX_VALUE;
  }

  @Override
  public DateTime getMinTime()
  {
    return getInterval().getStart();
  }

  @Override
  public DateTime getMaxTime()
  {
    return getInterval().getEnd().minus(1);
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    return null;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return RowBasedColumnSelectorFactory.getColumnCapabilities(rowSignature, column);
  }

  @Nullable
  @Override
  public String getColumnTypeName(String column)
  {
    final ColumnCapabilities columnCapabilities = getColumnCapabilities(column);
    return columnCapabilities != null ? columnCapabilities.asTypeString() : null;
  }

  @Override
  public int getNumRows()
  {
    // getNumRows is only used by tests and by segmentMetadataQuery (which
    // would be odd to call on inline datasources)
    // so no big deal if it doesn't always work.
    throw new UnsupportedOperationException("Cannot retrieve number of rows");
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  public Metadata getMetadata()
  {
    throw new UnsupportedOperationException("Cannot retrieve metadata");
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      final Interval queryInterval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    return makeCursors(rowSignature.getColumnNames(), filter, queryInterval, virtualColumns, gran, queryMetrics, 0, Long.MAX_VALUE, Collections.emptyList());
  }

  @Override
  public Sequence<Cursor> makeCursors(List<String> columns, Filter filter, Interval interval, VirtualColumns virtualColumns,
      Granularity gran, QueryMetrics<?> queryMetrics, long offset, long limit, List<OrderByColumnSpec> orderBy)
  {
    return cursorFactory.makeCursors(
        columns,
        filter,
        interval,
        virtualColumns,
        gran,
        queryMetrics,
        offset,
        limit,
        orderBy);
  }
}
