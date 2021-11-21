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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.SortedCursorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class VirtualDataSourceTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  // raw arrays, e.g. double[]{1.0, 2.0} on round trip get deserialized into List<Double>, so just model that
  private final List<Object[]> rows = ImmutableList.of(
      new Object[]{DateTimes.of("2000").getMillis(), "foo", 0d, ImmutableMap.of("n", "0"), ImmutableList.of(1.0, 2.0)},
      new Object[]{DateTimes.of("2000").getMillis(), "bar", 1d, ImmutableMap.of("n", "1"), ImmutableList.of(2.0, 4.0)},
      new Object[]{DateTimes.of("2000").getMillis(), "baz", 2d, ImmutableMap.of("n", "2"), ImmutableList.of(3.0, 6.0)}
  );

  private final List<String> expectedColumnNames = ImmutableList.of(
      ColumnHolder.TIME_COLUMN_NAME,
      "str",
      "double",
      "complex",
      "double_array"
  );

  private final ColumnType someComplex = new ColumnType(ValueType.COMPLEX, "foo", null);
  private final List<ColumnType> expectedColumnTypes = ImmutableList.of(
      ColumnType.LONG,
      ColumnType.STRING,
      ColumnType.DOUBLE,
      someComplex,
      ColumnType.DOUBLE_ARRAY
  );

  private final RowSignature expectedRowSignature;
  
  private final VirtualDataSource dataSource;

  public VirtualDataSourceTest()
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (int i = 0; i < expectedColumnNames.size(); i++) {
      builder.add(expectedColumnNames.get(i), expectedColumnTypes.get(i));
    }

    expectedRowSignature = builder.build();
    dataSource = VirtualDataSource.fromVirtualTable(
        Mockito.mock(SortedCursorFactory.class),
        expectedRowSignature,
        "internals");
  }

  @Test
  public void test_getCursorFactory()
  {
    Assert.assertNotNull(dataSource.getCursorFactory());
  }

  @Test
  public void test_getColumnNames()
  {
    Assert.assertEquals(expectedColumnNames, dataSource.getColumnNames());
  }

  @Test
  public void test_getTableNames()
  {
    Assert.assertEquals(1, dataSource.getTableNames().size());
    Assert.assertTrue(dataSource.getTableNames().contains("internals"));
  }

  @Test
  public void test_canScanOrdered()
  {
    Assert.assertTrue(dataSource.canScanOrdered(0, 0, Collections.emptyList()));
  }

  @Test
  public void test_getColumnTypes()
  {
    Assert.assertEquals(expectedColumnTypes, dataSource.getColumnTypes());
  }

  @Test
  public void test_getChildren()
  {
    Assert.assertEquals(Collections.emptyList(), dataSource.getChildren());
  }

  @Test
  public void test_getRowSignature()
  {
    Assert.assertEquals(
        RowSignature.builder()
                    .add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG)
                    .add("str", ColumnType.STRING)
                    .add("double", ColumnType.DOUBLE)
                    .add("complex", someComplex)
                    .add("double_array", ColumnType.DOUBLE_ARRAY)
                    .build(),
        dataSource.getRowSignature()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assert.assertFalse(dataSource.isCacheable(true));
    Assert.assertFalse(dataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assert.assertTrue(dataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assert.assertTrue(dataSource.isConcrete());
  }

  @Test
  public void test_rowAdapter()
  {
    final RowAdapter<Object[]> adapter = dataSource.rowAdapter();
    final Object[] row = rows.get(1);

    Assert.assertEquals(DateTimes.of("2000").getMillis(), adapter.timestampFunction().applyAsLong(row));
    Assert.assertEquals("bar", adapter.columnFunction("str").apply(row));
    Assert.assertEquals(1d, adapter.columnFunction("double").apply(row));
    Assert.assertEquals(ImmutableMap.of("n", "1"), adapter.columnFunction("complex").apply(row));
    Assert.assertEquals(ImmutableList.of(2.0, 4.0), adapter.columnFunction("double_array").apply(row));
  }

  @Test
  public void test_withChildren_empty()
  {
    Assert.assertSame(dataSource, dataSource.withChildren(Collections.emptyList()));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Cannot accept children");

    // Workaround so "withChildren" isn't flagged as unused in the DataSource interface.
    ((DataSource) dataSource).withChildren(ImmutableList.of(new TableDataSource("foo")));
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(VirtualDataSource.class)
                  .usingGetClass()
                  .withNonnullFields("cursorFactory", "signature", "tableNames")
                  .verify();
  }
}
