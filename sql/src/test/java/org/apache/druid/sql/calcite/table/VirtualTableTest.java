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

package org.apache.druid.sql.calcite.table;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.sql.calcite.table.VirtualTable.FilterPredicate;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToLongFunction;

public class VirtualTableTest extends InitializedNullHandlingTest
{
  RowSignature rowSignature;
  RowAdapter<Object[]> rowAdapter;
  VirtualColumns virtualColumns;

  @Before
  public void setup()
  {
    rowSignature = RowSignature.builder()
        .add("collong", ColumnType.LONG)
        .add("colstring", ColumnType.STRING)
        .build();
    rowAdapter = new RowAdapter<Object[]>() {
      @Override
      public ToLongFunction<Object[]> timestampFunction()
      {
        return (Object[] input) -> 0L;
      }

      @Override
      public Function<Object[], Object> columnFunction(String columnName)
      {
        if ("collong".equals(columnName)) {
          return (Object[] input) -> input[0];
        }
        if ("colstring".equals(columnName)) {
          return (Object[] input) -> input[1];
        }
        return null;
      }
    };
    virtualColumns = VirtualColumns.EMPTY;

  }

  @Test
  public void testFilterPredicateNullFilter()
  {
    Filter filter = null;

    FilterPredicate<Object[]> pred = new VirtualTable.FilterPredicate<>(
        rowSignature,
        rowAdapter,
        filter,
        virtualColumns);

    // any input should match
    Assert.assertTrue(pred.test(new Object[]{0L, "anystring"}));
    Assert.assertTrue(pred.test(new Object[]{-5L, "invalid"}));
  }

  @Test
  public void testFilterPredicateFalseFilter()
  {
    Filter filter = FalseFilter.instance();

    FilterPredicate<Object[]> pred = new VirtualTable.FilterPredicate<>(
        rowSignature,
        rowAdapter,
        filter,
        virtualColumns);

    // any input should fail
    Assert.assertFalse(pred.test(new Object[]{0L, "anystring"}));
    Assert.assertFalse(pred.test(new Object[]{-5L, "invalid"}));
  }

  @Test
  public void testFilterPredicateNormalFilter()
  {
    Filter filter = new SelectorFilter("colstring", "anystring");

    FilterPredicate<Object[]> pred = new VirtualTable.FilterPredicate<>(
        rowSignature,
        rowAdapter,
        filter,
        virtualColumns);

    Assert.assertTrue(pred.test(new Object[]{0L, "anystring"}));
    Assert.assertFalse(pred.test(new Object[]{-5L, "invalid"}));
  }

  @Test
  public void testRowComparatorEmptyAlwaysZero()
  {
    List<OrderByColumnSpec> orderBys = Collections.emptyList();

    Comparator<Object[]> comparator = new VirtualTable.RowOrderByComparator<>(orderBys, rowAdapter);

    Object[] aaa = new Object[]{0L, "aaa"};
    Object[] bbb = new Object[]{1L, "bbb"};

    Assert.assertEquals(0, comparator.compare(aaa, bbb));
    Assert.assertEquals(0, comparator.compare(bbb, aaa));
  }

  @Test
  public void testRowComparatorSimpleLong()
  {
    Comparator<Object[]> comparator;

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.asc("collong")),
        rowAdapter);

    Object[] aaa = new Object[]{0L, "aaa"};
    Object[] bbb = new Object[]{1L, "bbb"};

    Assert.assertTrue(comparator.compare(aaa, bbb) < 0);
    Assert.assertTrue(comparator.compare(bbb, aaa) > 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.desc("collong")),
        rowAdapter);

    Assert.assertFalse(comparator.compare(aaa, bbb) < 0);
    Assert.assertFalse(comparator.compare(bbb, aaa) > 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));
  }

  @Test
  public void testRowComparatorSimpleString()
  {
    Comparator<Object[]> comparator;

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.asc("colstring")),
        rowAdapter);

    Object[] aaa = new Object[]{0L, "aaa"};
    Object[] bbb = new Object[]{1L, "bbb"};

    Assert.assertTrue(comparator.compare(aaa, bbb) < 0);
    Assert.assertTrue(comparator.compare(bbb, aaa) > 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.desc("colstring")),
        rowAdapter);

    Assert.assertTrue(comparator.compare(aaa, bbb) > 0);
    Assert.assertTrue(comparator.compare(bbb, aaa) < 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));
  }

  @Test
  public void testRowComparatorCompound()
  {
    Comparator<Object[]> comparator;

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.asc("collong"), OrderByColumnSpec.desc("colstring")),
        rowAdapter);

    Object[] aaa = new Object[]{0L, "aaa"};
    Object[] bbb = new Object[]{0L, "bbb"};

    Assert.assertTrue(comparator.compare(aaa, bbb) > 0);
    Assert.assertTrue(comparator.compare(bbb, aaa) < 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));
    Assert.assertEquals(0, comparator.compare(bbb, bbb));

    comparator = new VirtualTable.RowOrderByComparator<>(
        Arrays.asList(OrderByColumnSpec.asc("collong"), OrderByColumnSpec.asc("colstring")),
        rowAdapter);

    Assert.assertTrue(comparator.compare(aaa, bbb) < 0);
    Assert.assertTrue(comparator.compare(bbb, aaa) > 0);
    Assert.assertEquals(0, comparator.compare(aaa, aaa));
    Assert.assertEquals(0, comparator.compare(bbb, bbb));
  }
}
