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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanResultValueCompoundComparatorTest
{
  private static QuerySegmentSpec intervalSpec;
  private static ScanResultValue s1;
  private static ScanResultValue s2;
  private static ScanResultValue s3;
  private static ScanResultValue s4;
  private static ScanResultValue s5;
  private static ScanResultValue s6;
  private static ScanResultValue s7;
  private static ScanResultValue s8;

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

    final ArrayList<String> colOneTwoThree = new ArrayList<>();
    colOneTwoThree.add("col1");
    colOneTwoThree.add("col2");
    colOneTwoThree.add("col3");

    ArrayList<Map<String, Object>> events6 = new ArrayList<>();
    Map<String, Object> event6 = new HashMap<String, Object>();
    event6.put("col1", "aa");
    event6.put("col2", "bb");
    event6.put("col3", Long.valueOf(10));
    events6.add(event6);

    s6 = new ScanResultValue(
        "segmentId",
        colOneTwoThree,
        events6
    );

    ArrayList<Map<String, Object>> events7 = new ArrayList<>();
    Map<String, Object> event7 = new HashMap<String, Object>();
    event7.put("col1", "aa");
    event7.put("col2", "bb");
    event7.put("col3", Long.valueOf(11));
    events7.add(event7);

    s7 = new ScanResultValue(
        "segmentId",
        colOneTwoThree,
        events7
    );

    ArrayList<Map<String, Object>> events8 = new ArrayList<>();
    Map<String, Object> event8 = new HashMap<String, Object>();
    event8.put("col1", "aa");
    event8.put("col2", "bb");
    event8.put("col3", Long.valueOf(12));
    events8.add(event8);

    s8 = new ScanResultValue(
        "segmentId",
        colOneTwoThree,
        events8
    );
  }

  @Test(expected = ISE.class)
  public void testTimeOrderingWithCompoundSortColumnMissing()
  {
    List<OrderByColumnSpec> orderBys = new ArrayList<>();
    orderBys.add(new OrderByColumnSpec(
        ColumnHolder.TIME_COLUMN_NAME,
        Direction.DESCENDING,
        StringComparators.NUMERIC));

    ScanQuery compoundOrderedScan = Druids.newScanQueryBuilder()
                                          .orderBy(orderBys)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();

    // s3 is missing the time column

    Sequence<ScanResultValue> result = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s1)),
            Sequences.simple(ImmutableList.of(s2)),
            Sequences.simple(ImmutableList.of(s3))
        )
    ).flatMerge(seq -> seq, compoundOrderedScan.getResultOrdering());

    List<ScanResultValue> resultList = result.toList();
  }

  @Test
  public void testCompoundSort()
  {
    List<OrderByColumnSpec> orderBys = new ArrayList<>();
    orderBys.add(new OrderByColumnSpec(
        "col1",
        Direction.DESCENDING,
        StringComparators.LEXICOGRAPHIC));
    orderBys.add(new OrderByColumnSpec(
        "col2",
        Direction.DESCENDING,
        StringComparators.LEXICOGRAPHIC));
    orderBys.add(new OrderByColumnSpec(
        "col3",
        Direction.ASCENDING,
        StringComparators.NUMERIC));

    ScanQuery query = Druids.newScanQueryBuilder()
                                          .orderBy(orderBys)
                                          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_LIST)
                                          .dataSource("some src")
                                          .intervals(intervalSpec)
                                          .build();

    // for s6, s7, s8 
    // col1 and col2 are the same, col3 is sorted

    Sequence<ScanResultValue> result = Sequences.simple(
        ImmutableList.of(
            Sequences.simple(ImmutableList.of(s7)),
            Sequences.simple(ImmutableList.of(s8)),
            Sequences.simple(ImmutableList.of(s6))
        )
    ).flatMerge(seq -> seq, query.getResultOrdering());

    List<ScanResultValue> resultList = result.toList();
    Assert.assertEquals(s6, resultList.get(0));
    Assert.assertEquals(s7, resultList.get(1));
    Assert.assertEquals(s8, resultList.get(2));
  }

  @Test
  public void testNonTimeOrderingDesc()
  {
    List<OrderByColumnSpec> orderBys = new ArrayList<>();
    orderBys.add(new OrderByColumnSpec(
        "yah",
        Direction.DESCENDING,
        StringComparators.LEXICOGRAPHIC));

    ScanQuery compoundOrderedScan = Druids.newScanQueryBuilder()
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
    ).flatMerge(seq -> seq, compoundOrderedScan.getResultOrdering());

    List<ScanResultValue> resultList = result.toList();
    Assert.assertEquals(s5, resultList.get(0));
    Assert.assertEquals(s3, resultList.get(1));
    Assert.assertEquals(s4, resultList.get(2));
  }
}
