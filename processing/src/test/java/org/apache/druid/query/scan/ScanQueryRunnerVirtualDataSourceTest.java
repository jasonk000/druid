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

import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.VirtualDataSource;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.ListBasedCursor;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.SortedCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.VirtualSegment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

/**
 *
 */
public class ScanQueryRunnerVirtualDataSourceTest
{
  RowSignature rowSignature = null;
  List<Map<String, Object>> expectedValues = null;

  @Before
  public void setup()
  {
    rowSignature = RowSignature.builder()
        .add("collong1", ColumnType.LONG)
        .add("collong2", ColumnType.LONG)
        .add("colstring1", ColumnType.STRING)
        .add("colstring2", ColumnType.STRING)
        .build();

    HashMap<String, Object> row1 = new HashMap<>();
    row1.put("collong1", 11);
    row1.put("collong2", 12);
    row1.put("colstring1", "str11");
    row1.put("colstring2", "str12");
    HashMap<String, Object> row2 = new HashMap<>();
    row2.put("collong1", 21);
    row2.put("collong2", 22);
    row2.put("colstring1", "str21");
    row2.put("colstring2", "str22");
    HashMap<String, Object> row3 = new HashMap<>();
    row3.put("collong1", 31);
    row3.put("collong2", 32);
    row3.put("colstring1", "str31");
    row3.put("colstring2", "str32");
    HashMap<String, Object> row4 = new HashMap<>();
    row4.put("collong1", 41);
    row4.put("collong2", 42);
    row4.put("colstring1", "str41");
    row4.put("colstring2", "str42");
    expectedValues = Arrays.asList(row1, row2, row3, row4);
  }

  private static final ScanQueryQueryToolChest TOOL_CHEST = new ScanQueryQueryToolChest(
      new ScanQueryConfig(),
      DefaultGenericQueryMetricsFactory.instance()
  );

  static <T> SortedCursorFactory sortedCursorFactoryForList(List<T> results, ColumnSelectorFactory csf, SettableSupplier<T> rowSupplier)
  {
    SortedCursorFactory scf = new SortedCursorFactory() {
      @Override
      public Sequence<Cursor> makeCursors(List<String> columns, Filter filter, Interval interval,
          VirtualColumns virtualColumns, Granularity gran, QueryMetrics<?> queryMetrics, long offset, long limit,
          List<OrderByColumnSpec> orderBy)
      {
        final ListBasedCursor<T> cursor = new ListBasedCursor<>(
            results,
            rowSupplier,
            csf, gran, interval);
        return Sequences.simple(Collections.singletonList(cursor));
      }
    };

    return scf;
  }

  static class MapRowAdapter implements RowAdapter<Map<String, Object>>
  {
    @Override
    public ToLongFunction<Map<String, Object>> timestampFunction()
    {
      return (Map<String, Object> m) -> Long.valueOf(m.getOrDefault("__time", "0").toString());
    }

    @Override
    public java.util.function.Function<Map<String, Object>, Object> columnFunction(String columnName)
    {
      return (Map<String, Object> m) -> m.get(columnName);
    }
  }

  @Test
  public void testVirtualQueryReturnsUnmodifiedData()
  {
    final SettableSupplier<Map<String, Object>> rowSupplier = new SettableSupplier<>();
    final RowBasedColumnSelectorFactory<Map<String, Object>> csf =
        RowBasedColumnSelectorFactory.create(new MapRowAdapter(), rowSupplier::get, rowSignature, true);

    SortedCursorFactory scf = sortedCursorFactoryForList(expectedValues, csf, rowSupplier);

    // verify that a query from a virtualised datasource runs the query and passes
    // through the results without any changes

    VirtualDataSource vds = VirtualDataSource.fromVirtualTable(scf, rowSignature, "test");

    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(vds)
        .columns("collong1", "colstring1")
        .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .legacy(false)
        .build();

    QueryRunner runner = new ScanQueryRunnerFactory(
        TOOL_CHEST,
        new ScanQueryEngine(),
        new ScanQueryConfig())
        .createRunner(new VirtualSegment<>(
            SegmentId.dummy("segment"),
            vds.getCursorFactory(),
            vds.getRowSignature()));

    List<ScanResultValue> scanResults = runner.run(QueryPlus.wrap(query)).toList();

    List<List<Object>> results = (List<List<Object>>) scanResults.get(0).getEvents();

    Assert.assertEquals(4, results.size());
    Assert.assertEquals(11, results.get(0).get(0));
    Assert.assertEquals("str11", results.get(0).get(1));
    Assert.assertEquals(21, results.get(1).get(0));
    Assert.assertEquals("str21", results.get(1).get(1));
    Assert.assertEquals(31, results.get(2).get(0));
    Assert.assertEquals("str31", results.get(2).get(1));
    Assert.assertEquals(41, results.get(3).get(0));
    Assert.assertEquals("str41", results.get(3).get(1));
  }

  @Test
  public void testVirtualQueryPassesOffsetAndLimit()
  {
    AtomicLong atomicOffset = new AtomicLong(-1);
    AtomicLong atomicLimit = new AtomicLong(-1);

    final SettableSupplier<Map<String, Object>> rowSupplier = new SettableSupplier<>();
    final RowBasedColumnSelectorFactory<Map<String, Object>> csf =
        RowBasedColumnSelectorFactory.create(new MapRowAdapter(), rowSupplier::get, rowSignature, true);

    SortedCursorFactory scf = new SortedCursorFactory() {
      @Override
      public Sequence<Cursor> makeCursors(List<String> columns, Filter filter, Interval interval,
          VirtualColumns virtualColumns, Granularity gran, QueryMetrics<?> queryMetrics, long offset, long limit,
          List<OrderByColumnSpec> orderBy)
      {
        atomicOffset.set(offset);
        atomicLimit.set(limit);
        // empty List ok, we do not intend to _check_ the result
        final ListBasedCursor cursor =
            new ListBasedCursor(Collections.emptyList(), rowSupplier, csf, gran, interval);
        return Sequences.simple(Collections.singletonList(cursor));
      }
    };

    // verify that a query from a virtualised datasource runs the query and passes
    // through the results without any changes

    VirtualDataSource vds = VirtualDataSource.fromVirtualTable(scf, rowSignature, "test");

    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(vds)
        .columns("collong1", "colstring1")
        .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
        .offset(50)
        .limit(25)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .legacy(false)
        .build();

    QueryRunner runner = new ScanQueryRunnerFactory(
        TOOL_CHEST,
        new ScanQueryEngine(),
        new ScanQueryConfig())
        .createRunner(new VirtualSegment<>(
            SegmentId.dummy("segment"),
            vds.getCursorFactory(),
            vds.getRowSignature()));

    List<ScanResultValue> scanResults = runner.run(QueryPlus.wrap(query)).toList();

    Assert.assertEquals(0, scanResults.size());
    Assert.assertEquals(50, atomicOffset.get());
    Assert.assertEquals(25, atomicLimit.get());
  }

  @Test
  public void testVirtualQueryReturnsUnmodifiedDataWithOffsetLimitSort()
  {
    final SettableSupplier<Map<String, Object>> rowSupplier = new SettableSupplier<>();
    final RowBasedColumnSelectorFactory<Map<String, Object>> csf =
        RowBasedColumnSelectorFactory.create(new MapRowAdapter(), rowSupplier::get, rowSignature, true);

    SortedCursorFactory scf = sortedCursorFactoryForList(expectedValues, csf, rowSupplier);

    // verify that a query from a virtualised datasource runs the query and passes
    // through the results without any changes

    VirtualDataSource vds = VirtualDataSource.fromVirtualTable(scf, rowSignature, "test");

    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource(vds)
        .columns("collong1", "colstring1")
        .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
        .offset(1)
        .limit(2)
        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .legacy(false)
        .build();

    QueryRunner runner = new ScanQueryRunnerFactory(
        TOOL_CHEST,
        new ScanQueryEngine(),
        new ScanQueryConfig())
        .createRunner(new VirtualSegment<>(
            SegmentId.dummy("segment"),
            vds.getCursorFactory(),
            vds.getRowSignature()));

    List<ScanResultValue> scanResults = runner.run(QueryPlus.wrap(query)).toList();

    List<List<Object>> results = (List<List<Object>>) scanResults.get(0).getEvents();

    // important:
    // the offset 1 limit 2 are passed through to the query, and the ScanQuery
    // should return the query results assuming the underlying layer has
    // correctly done the sort

    // so, offset/limit here should result still in the first two elements of the
    // list returned, since that's what the underlying datasource returned

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(11, results.get(0).get(0));
    Assert.assertEquals("str11", results.get(0).get(1));
    Assert.assertEquals(21, results.get(1).get(0));
    Assert.assertEquals("str21", results.get(1).get(1));
  }
}
