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

package org.apache.druid.query.scan;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.Comparator;
import java.util.List;

/**
 * This comparator class supports comparisons of ScanResultValues based on the timestamp of their first event.  Since
 * only the first event is looked at, this Comparator is especially useful for unbatched ScanResultValues (such as in
 * {@link ScanQueryQueryToolChest#mergeResults(org.apache.druid.query.QueryRunner <ScanResultValue>)}.  The comparator takes a scanQuery as
 * a parameter so that it knows the result format (list or compactedList) of Object ScanResultValue#events.  It uses
 * this result format to perform a bunch of type casts on the object to get the timestamp then compares the timestamps.
 */
public class ScanResultValueTimestampComparator implements Comparator<ScanResultValue>
{
  private final ScanQuery scanQuery;
  private final List<OrderByColumnSpec> orderBy;
  private final String TIME = ColumnHolder.TIME_COLUMN_NAME;

  public ScanResultValueTimestampComparator(ScanQuery scanQuery)
  {
    this.scanQuery = scanQuery;
    Preconditions.checkNotNull(scanQuery.getOrderBy(), "getOrderBy");
    this.orderBy = scanQuery.getOrderBy();
    Preconditions.checkArgument(orderBy.size() == 1, "orderBy size should be one");
    Preconditions.checkNotNull(
        OrderByColumnSpec.getOrderByForDimName(orderBy, TIME),
        "orderBy should have time column only");
  }

  @Override
  public int compare(ScanResultValue o1, ScanResultValue o2)
  {
    int comparison = Longs.compare(
        o1.getFirstEventTimestamp(scanQuery.getResultFormat()),
        o2.getFirstEventTimestamp(scanQuery.getResultFormat()));
    if (OrderByColumnSpec.getOrderByForDimName(orderBy, TIME).getDirection() == Direction.ASCENDING) {
      return comparison;
    }
    return comparison * -1;
  }
}
