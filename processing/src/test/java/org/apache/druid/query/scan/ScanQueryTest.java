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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryUtils;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScanQueryTest
{
  private static QuerySegmentSpec intervalSpec;
  private static ScanResultValue s1;
  private static ScanResultValue s2;
  private static ScanResultValue s3;
  private static ScanResultValue s4;
  private static ScanResultValue s5;

  @BeforeClass
  public static void setup()
  {
    intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(
            new Interval(DateTimes.of("2012-01-01"), DateTimes.of("2012-01-01").plusHours(1))
        )
    );

    ArrayList<Map<String, Object>> events1 = new ArrayList<>();
    events1.add(Collections.singletonMap(ColumnHolder.TIME_COLUMN_NAME, new Long(42)));

    s1 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events1
    );

    ArrayList<Map<String, Object>> events2 = new ArrayList<>();
    events2.add(Collections.singletonMap(ColumnHolder.TIME_COLUMN_NAME, new Long(43)));

    s2 = new ScanResultValue(
        "segmentId",
        Collections.singletonList(ColumnHolder.TIME_COLUMN_NAME),
        events2
    );

    // ScanResultValue s3 has no time column
    ArrayList<Map<String, Object>> events3 = new ArrayList<>();
    events3.add(Collections.singletonMap("yah", "yeet"));

    s3 = new ScanResultValue(
        "segmentId",
        Collections.singletonList("yah"),
        events3
    );

    ArrayList<Map<String, Object>> events4 = new ArrayList<>();
    events4.add(Collections.singletonMap("yah", "xxxxx"));

    s4 = new ScanResultValue(
        "segmentId",
        Collections.singletonList("yah"),
        events4
    );

    ArrayList<Map<String, Object>> events5 = new ArrayList<>();
    events5.add(Collections.singletonMap("yah", "zzzz"));

    s5 = new ScanResultValue(
        "segmentId",
        Collections.singletonList("yah"),
        events5
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAscendingScanQueryWithInvalidColumns()
  {
    Druids.newScanQueryBuilder()
          .orderBy(QueryUtils.newOrderByTimeSpec(Direction.ASCENDING))
          .columns(ImmutableList.of("not time", "also not time"))
          .dataSource("source")
          .intervals(intervalSpec)
          .build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDescendingScanQueryWithInvalidColumns()
  {
    Druids.newScanQueryBuilder()
          .orderBy(QueryUtils.newOrderByTimeSpec(Direction.DESCENDING))
          .columns(ImmutableList.of("not time", "also not time"))
          .dataSource("source")
          .intervals(intervalSpec)
          .build();
  }

  // No assertions because we're checking that no IllegalArgumentExceptions are thrown
  @Test
  public void testValidScanQueryInitialization()
  {
    List<List<OrderByColumnSpec>> nonOrderedOrders = Arrays.asList(null, QueryUtils.newOrderByTimeSpec(null));

    for (List<OrderByColumnSpec> orderBy : nonOrderedOrders) {
      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns(ImmutableList.of("not time"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();


      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns(ImmutableList.of())
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns(ImmutableList.of("__time"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();
    }

    Set<List<OrderByColumnSpec>> orderedOrders = ImmutableSet.of(
        QueryUtils.newOrderByTimeSpec(Direction.ASCENDING),
        QueryUtils.newOrderByTimeSpec(Direction.DESCENDING));

    for (List<OrderByColumnSpec> orderBy : orderedOrders) {
      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns((List<String>) null)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns(ImmutableList.of())
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .dataSource("source")
            .intervals(intervalSpec)
            .build();

      Druids.newScanQueryBuilder()
            .orderBy(orderBy)
            .columns(ImmutableList.of("__time", "col2"))
            .dataSource("source")
            .intervals(intervalSpec)
            .build();
    }
  }

  // Validates that getResultOrdering will work for the broker n-way merge
  @Test
  public void testMergeSequenceForResults()
  {
    // Should be able to handle merging s1, s2, s3
    ScanQuery noOrderScan = Druids.newScanQueryBuilder()
                                  .orderBy(QueryUtils.newOrderByTimeSpec(null))
                                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                  .dataSource("some src")
                                  .intervals(intervalSpec)
                                  .build();

    // Should only handle s1 and s2
    ScanQuery descendingOrderScan = Druids.newScanQueryBuilder()
                                          .orderBy(QueryUtils.newOrderByTimeSpec(Direction.DESCENDING))
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();

    // Should only handle s1 and s2
    ScanQuery ascendingOrderScan = Druids.newScanQueryBuilder()
                                         .orderBy(QueryUtils.newOrderByTimeSpec(Direction.ASCENDING))
                                         .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                         .dataSource("some src")
                                         .intervals(intervalSpec)
                                         .build();
    // No Order
    Sequence<ScanResultValue> noOrderSeq =
        Sequences.simple(
            ImmutableList.of(
                Sequences.simple(ImmutableList.of(s1, s3)),
                Sequences.simple(ImmutableList.of(s2))
            )
        ).flatMerge(seq -> seq, noOrderScan.getResultOrdering());

    List<ScanResultValue> noOrderList = noOrderSeq.toList();
    Assert.assertEquals(3, noOrderList.size());


    // Ascending
    Sequence<ScanResultValue> ascendingOrderSeq = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2))
        )
    ).flatMerge(seq -> seq, ascendingOrderScan.getResultOrdering());

    List<ScanResultValue> ascendingList = ascendingOrderSeq.toList();
    Assert.assertEquals(2, ascendingList.size());
    Assert.assertEquals(s1, ascendingList.get(0));
    Assert.assertEquals(s2, ascendingList.get(1));

    // Descending
    Sequence<ScanResultValue> descendingOrderSeq = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2))
        )
    ).flatMerge(seq -> seq, descendingOrderScan.getResultOrdering());

    List<ScanResultValue> descendingList = descendingOrderSeq.toList();
    Assert.assertEquals(2, descendingList.size());
    Assert.assertEquals(s2, descendingList.get(0));
    Assert.assertEquals(s1, descendingList.get(1));
  }

  @Test(expected = ISE.class)
  public void testTimeOrderingWithoutTimeColumn()
  {
    ScanQuery descendingOrderScan = Druids.newScanQueryBuilder()
                                          .orderBy(QueryUtils.newOrderByTimeSpec(Direction.DESCENDING))
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();
    // This should fail because s3 doesn't have a timestamp
    Sequence<ScanResultValue> borkedSequence = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2, s3))
        )
    ).flatMerge(seq -> seq, descendingOrderScan.getResultOrdering());

    // This should throw an ISE
    List<ScanResultValue> res = borkedSequence.toList();
  }

  @Test
  public void testNonTimeOrderingAsc()
  {
    // test that query can set up a result ordering with a non-time-based column

    List<OrderByColumnSpec> orderBys = new ArrayList<>();
    orderBys.add(new OrderByColumnSpec(
        "yah",
        Direction.ASCENDING,
        StringComparators.LEXICOGRAPHIC));

    ScanQuery orderedScan = Druids.newScanQueryBuilder()
                                          .orderBy(orderBys)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();

    // s3 (yeet) s4 (xxxx) s5 (zzzz)

    Sequence<ScanResultValue> result = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s3)),
            Sequences.simple(ImmutableList.of(s4)),
            Sequences.simple(ImmutableList.of(s5))
        )
    ).flatMerge(seq -> seq, orderedScan.getResultOrdering());

    List<ScanResultValue> resultList = result.toList();
    Assert.assertEquals(s4, resultList.get(0));
    Assert.assertEquals(s3, resultList.get(1));
    Assert.assertEquals(s5, resultList.get(2));
  }

  @Test
  public void testGetRequiredColumnsWithNoColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .orderBy(QueryUtils.newOrderByTimeSpec(Direction.DESCENDING))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .build();

    Assert.assertNull(query.getRequiredColumns());
  }

  @Test
  public void testGetRequiredColumnsWithEmptyColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .orderBy(QueryUtils.newOrderByTimeSpec(Direction.DESCENDING))
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .columns(Collections.emptyList())
              .build();

    Assert.assertNull(query.getRequiredColumns());
  }

  @Test
  public void testGetRequiredColumnsWithColumns()
  {
    final ScanQuery query =
        Druids.newScanQueryBuilder()
              .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
              .dataSource("some src")
              .intervals(intervalSpec)
              .columns("foo", "bar")
              .build();

    Assert.assertEquals(ImmutableSet.of("__time", "foo", "bar"), query.getRequiredColumns());
  }
}
