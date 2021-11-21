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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * This comparator class supports comparisons of ScanResultValues based on the timestamp of their first event.  Since
 * only the first event is looked at, this Comparator is especially useful for unbatched ScanResultValues (such as in
 * {@link ScanQueryQueryToolChest#mergeResults(org.apache.druid.query.QueryRunner <ScanResultValue>)}.  The comparator takes a scanQuery as
 * a parameter so that it knows the result format (list or compactedList) of Object ScanResultValue#events.  It uses
 * this result format to perform a bunch of type casts on the object to get the timestamp then compares the timestamps.
 */
public class ScanResultValueCompoundComparator implements Comparator<ScanResultValue>
{
  private static final EmittingLogger log = new EmittingLogger(ScanResultValueCompoundComparator.class);

  private final List<String> columns;
  private final List<OrderByColumnSpec> orderBy;
  private final BiFunction<List<Object>, String, Comparable> fieldReader;

  public ScanResultValueCompoundComparator(List<String> columns, List<OrderByColumnSpec> orderBy, ResultFormat rf)
  {
    Preconditions.checkNotNull(columns, "columns");
    Preconditions.checkNotNull(orderBy, "orderBy");
    Preconditions.checkNotNull(rf, "rf");

    this.columns = columns;
    this.orderBy = orderBy;

    if (rf.equals(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)) {
      fieldReader = this::readFromCompactList;
    } else if (rf.equals(ResultFormat.RESULT_FORMAT_LIST)) {
      fieldReader = this::readFromList;
    } else {
      // impossible?
      fieldReader = null;
    }
  }

  public ScanResultValueCompoundComparator(ScanQuery scanQuery)
  {
    this(scanQuery.getColumns(), scanQuery.getOrderBy(), scanQuery.getResultFormat());
  }

  Comparable checkedObjectToComparable(Object o, String column)
  {
    if (o == null) {
      throw new ISE("value was null for column [%s]", column);
    }
    if (!(o instanceof Comparable)) {
      throw new ISE("result not comparable for column [%s]", column);
    }
    return (Comparable) o;
  }

  Comparable readFromCompactList(List<Object> rawEvents, String column)
  {
    int columnNumber = columns.indexOf(column);
    Object result = rawEvents.get(columnNumber);
    return checkedObjectToComparable(result, column);
  }

  Comparable readFromList(List<Object> rawEvents, String column)
  {
    Map<String, Object> events = (Map<String, Object>) rawEvents.get(0);
    Object result = events.get(column);
    return checkedObjectToComparable(result, column);
  }

  @Override
  public int compare(ScanResultValue o1, ScanResultValue o2)
  {
    for (OrderByColumnSpec colSpec : orderBy) {
      Comparable val1 = fieldReader.apply((List<Object>) o1.getEvents(), colSpec.getDimension());
      Comparable val2 = fieldReader.apply((List<Object>) o2.getEvents(), colSpec.getDimension());

      final int comparison;
      if (colSpec.getDirection() == Direction.ASCENDING) {
        comparison = val1.compareTo(val2);
      } else {
        comparison = val2.compareTo(val1);
      }

      if (comparison != 0) {
        return comparison;
      }
    }

    return 0;
  }
}
