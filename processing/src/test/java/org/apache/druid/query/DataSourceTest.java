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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataSourceTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerialization() throws IOException
  {
    DataSource dataSource = new TableDataSource("somedatasource");
    String json = JSON_MAPPER.writeValueAsString(dataSource);
    DataSource serdeDataSource = JSON_MAPPER.readValue(json, DataSource.class);
    Assert.assertEquals(dataSource, serdeDataSource);
  }

  @Test
  public void testLegacyDataSource() throws IOException
  {
    DataSource dataSource = JSON_MAPPER.readValue("\"somedatasource\"", DataSource.class);
    Assert.assertEquals(new TableDataSource("somedatasource"), dataSource);
  }

  @Test
  public void testTableDataSource() throws IOException
  {
    DataSource dataSource = JSON_MAPPER.readValue(
        "{\"type\":\"table\", \"name\":\"somedatasource\"}",
        DataSource.class
    );
    Assert.assertEquals(new TableDataSource("somedatasource"), dataSource);
  }

  @Test
  public void testQueryDataSource() throws IOException
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    String dataSourceJSON = "{\"type\":\"query\", \"query\":" + JSON_MAPPER.writeValueAsString(query) + "}";

    DataSource dataSource = JSON_MAPPER.readValue(dataSourceJSON, DataSource.class);
    Assert.assertEquals(new QueryDataSource(query), dataSource);
  }

  @Test
  public void testUnionDataSource() throws Exception
  {
    DataSource dataSource = JSON_MAPPER.readValue(
        "{\"type\":\"union\", \"dataSources\":[\"ds1\", \"ds2\"]}",
        DataSource.class
    );
    Assert.assertTrue(dataSource instanceof UnionDataSource);
    Assert.assertEquals(
        Lists.newArrayList(new TableDataSource("ds1"), new TableDataSource("ds2")),
        Lists.newArrayList(((UnionDataSource) dataSource).getDataSources())
    );
    Assert.assertEquals(
        ImmutableSet.of("ds1", "ds2"),
        dataSource.getTableNames()
    );

    final DataSource serde = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(dataSource), DataSource.class);
    Assert.assertEquals(dataSource, serde);
  }

  @Test
  public void testCanScanOrderedNoSort()
  {
    DataSource dataSource = Mockito.spy(DataSource.class);
    Assert.assertTrue(dataSource.canScanOrdered(0, 0, Collections.emptyList()));
  }

  @Test
  public void testCanScanOrderedSortByTime()
  {
    DataSource dataSource = Mockito.spy(DataSource.class);
    List<OrderByColumnSpec> ordering = QueryUtils.newOrderByTimeSpec(Direction.DESCENDING);
    Assert.assertTrue(dataSource.canScanOrdered(0, 0, ordering));
  }

  @Test
  public void testCannotScanOrderedByTimeWrongSpec()
  {
    DataSource dataSource = Mockito.spy(DataSource.class);
    List<OrderByColumnSpec> ordering = new ArrayList<OrderByColumnSpec>();
    ordering.add(new OrderByColumnSpec(
        ColumnHolder.TIME_COLUMN_NAME,
        Direction.DESCENDING,
        StringComparators.LEXICOGRAPHIC));
    Assert.assertFalse(dataSource.canScanOrdered(0, 0, ordering));
  }

  @Test
  public void testCannotScanOrderedByOtherColumn()
  {
    DataSource dataSource = Mockito.spy(DataSource.class);
    List<OrderByColumnSpec> ordering = new ArrayList<OrderByColumnSpec>();
    ordering.add(new OrderByColumnSpec(
        "wrongcol",
        Direction.DESCENDING,
        StringComparators.NUMERIC));
    Assert.assertFalse(dataSource.canScanOrdered(0, 0, ordering));
  }

  @Test
  public void testCannotScanOrderedCompoundIncludingTime()
  {

    DataSource dataSource = Mockito.spy(DataSource.class);
    List<OrderByColumnSpec> ordering = new ArrayList<OrderByColumnSpec>();
    ordering.add(new OrderByColumnSpec(
        ColumnHolder.TIME_COLUMN_NAME,
        Direction.DESCENDING,
        StringComparators.NUMERIC));
    ordering.add(new OrderByColumnSpec(
        "wrongcol",
        Direction.DESCENDING,
        StringComparators.NUMERIC));
    Assert.assertFalse(dataSource.canScanOrdered(0, 0, ordering));
  }
}
