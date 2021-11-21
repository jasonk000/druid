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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.ListBasedCursor;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.SortedCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.table.VirtualTable;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This table contains a row per supervisor task.
 */
class SupervisorsTable extends VirtualTable
{
  static final RowSignature SUPERVISOR_SIGNATURE = RowSignature
      .builder()
      .add("supervisor_id", ColumnType.STRING)
      .add("state", ColumnType.STRING)
      .add("detailed_state", ColumnType.STRING)
      .add("healthy", ColumnType.LONG)
      .add("type", ColumnType.STRING)
      .add("source", ColumnType.STRING)
      .add("suspended", ColumnType.LONG)
      .add("spec", ColumnType.STRING)
      .build();

  private final DruidLeaderClient druidLeaderClient;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;

  public SupervisorsTable(
      DruidLeaderClient druidLeaderClient,
      ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper
  )
  {
    super(SUPERVISOR_SIGNATURE, "supervisors");
    this.druidLeaderClient = druidLeaderClient;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public SortedCursorFactory newCursorFactory(AuthenticationResult authenticationResult)
  {
    return new SupervisorCursorFactory(authenticationResult);
  }

  static Iterable<ResourceAction> resourceActionGenerator(SupervisorStatus supervisor)
  {
    return Collections.singletonList(AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR.apply(supervisor.getSource()));
  }

  private static class SupervisorStatusRowAdapter implements RowAdapter<SupervisorStatus>
  {
    @Override
    public ToLongFunction<SupervisorStatus> timestampFunction()
    {
      return (SupervisorStatus s) -> 0L;
    }

    @Override
    public Function<SupervisorStatus, Object> columnFunction(String columnName)
    {
      switch (columnName) {
        case "supervisor_id":
          return SupervisorStatus::getId;
        case "state":
          return SupervisorStatus::getState;
        case "detailed_state":
          return SupervisorStatus::getDetailedState;
        case "healthy":
          return (SupervisorStatus s) -> s.isHealthy() ? 1L : 0L;
        case "type":
          return SupervisorStatus::getType;
        case "source":
          return SupervisorStatus::getSource;
        case "suspended":
          return (SupervisorStatus s) -> s.isSuspended() ? 1L : 0L;
        case "spec":
          return SupervisorStatus::getSpecString;
        default:
          return (SupervisorStatus s) -> null;
      }
    }
  }

  private class SupervisorCursorFactory implements SortedCursorFactory
  {
    final Predicate<SupervisorStatus> authorizedFilter;

    public SupervisorCursorFactory(AuthenticationResult authenticationResult)
    {
      Preconditions.checkNotNull(authenticationResult, "authenticationResult");
      authorizedFilter = AuthorizationUtils.createPredicateForAuthorizedResources(
          authenticationResult,
          SupervisorsTable::resourceActionGenerator,
          authorizerMapper);
    }

    @Override
    public Sequence<Cursor> makeCursors(List<String> columns, Filter filter, Interval interval, VirtualColumns virtualColumns,
            Granularity gran, QueryMetrics<?> queryMetrics, long offset, long limit, List<OrderByColumnSpec> orderBy)
    {
      final Stream<SupervisorStatus> supervisors = getSupervisors(druidLeaderClient, jsonMapper);
      final List<SupervisorStatus> results = supervisors
          .filter(authorizedFilter)
          .filter(new FilterPredicate<>(getRowSignature(), new SupervisorStatusRowAdapter(), filter, virtualColumns))
          .sorted(new RowOrderByComparator<>(orderBy, new SupervisorStatusRowAdapter()))
          .skip(offset)
          .limit(limit)
          .collect(Collectors.toCollection(ArrayList::new));

      final SettableSupplier<SupervisorStatus> rowSupplier = new SettableSupplier<>();
      final RowBasedColumnSelectorFactory<SupervisorStatus> csf = RowBasedColumnSelectorFactory.create(
          new SupervisorStatusRowAdapter(),
          rowSupplier::get,
          rowSignature,
          true);

      final ListBasedCursor<SupervisorStatus> cursor = new ListBasedCursor<>(results, rowSupplier, csf, gran, interval);
      return Sequences.simple(Collections.singletonList(cursor));
    }
  }

  // Note that overlord must be up to get supervisor tasks, otherwise queries to sys.supervisors table
  // will fail with internal server error (HTTP 500)
  private static Stream<SupervisorStatus> getSupervisors(
      DruidLeaderClient indexingServiceClient,
      ObjectMapper jsonMapper
  )
  {
    JsonParserIterator<SupervisorStatus> jsonParserIterator = SystemSchema.getThingsFromLeaderNode(
        "/druid/indexer/v1/supervisor?system",
        new TypeReference<SupervisorStatus>()
        {
        },
        indexingServiceClient,
        jsonMapper
    );
    return StreamSupport.stream(
          Spliterators.spliteratorUnknownSize(jsonParserIterator, Spliterator.ORDERED), false)
          .onClose(() -> closeOrThrow(jsonParserIterator));
  }
}
