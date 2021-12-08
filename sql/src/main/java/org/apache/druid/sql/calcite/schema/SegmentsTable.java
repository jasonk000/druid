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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.shaded.com.google.common.base.Preconditions;
import org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.EmittingLogger;
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
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentWithOvershadowedStatus;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This table contains a row per supervisor task.
 */
class SegmentsTable extends VirtualTable
{
  private static final EmittingLogger log = new EmittingLogger(SegmentsTable.class); 

  /**
   * Booleans constants represented as long type,
   * where 1 = true and 0 = false to make it easy to count number of segments
   * which are published, available etc.
   */
  private static final long IS_PUBLISHED_FALSE = 0L;
  private static final long IS_PUBLISHED_TRUE = 1L;
  private static final long IS_AVAILABLE_TRUE = 1L;
  private static final long IS_OVERSHADOWED_FALSE = 0L;
  private static final long IS_OVERSHADOWED_TRUE = 1L;
  private static final long IS_REALTIME_FALSE = 0L;

  static final RowSignature SEGMENTS_SIGNATURE = RowSignature
      .builder()
      .add("segment_id", ColumnType.STRING)
      .add("datasource", ColumnType.STRING)
      .add("start", ColumnType.STRING)
      .add("end", ColumnType.STRING)
      .add("size", ColumnType.LONG)
      .add("version", ColumnType.STRING)
      .add("partition_num", ColumnType.LONG)
      .add("num_replicas", ColumnType.LONG)
      .add("num_rows", ColumnType.LONG)
      .add("is_published", ColumnType.LONG)
      .add("is_available", ColumnType.LONG)
      .add("is_realtime", ColumnType.LONG)
      .add("is_overshadowed", ColumnType.LONG)
      .add("shard_spec", ColumnType.STRING)
      .add("dimensions", ColumnType.STRING)
      .add("metrics", ColumnType.STRING)
      .add("last_compaction_state", ColumnType.STRING)
      .build();

  private final DruidSchema druidSchema;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final MetadataSegmentView metadataView;

  public SegmentsTable(
      DruidSchema druidSchemna,
      MetadataSegmentView metadataView,
      ObjectMapper jsonMapper,
      AuthorizerMapper authorizerMapper
  )
  {
    super(SEGMENTS_SIGNATURE, "segments");
    this.druidSchema = druidSchemna;
    this.metadataView = metadataView;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
  }

  @Override
  public SortedCursorFactory newCursorFactory(AuthenticationResult authenticationResult)
  {
    return new SegmentsCursorFactory(authenticationResult);
  }

  static Iterable<ResourceAction> resourceActionGenerator(SegmentRecord record)
  {
    return Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(record.getDatasource()));
  }

  private static class SegmentRecordRowAdapter implements RowAdapter<SegmentRecord>
  {
    @Override
    public ToLongFunction<SegmentRecord> timestampFunction()
    {
      return (SegmentRecord r) -> 0L;
    }

    @Override
    public Function<SegmentRecord, Object> columnFunction(String columnName)
    {
      switch (columnName) {
        case "segment_id":
          return SegmentRecord::getSegmentId;
        case "datasource":
          return SegmentRecord::getDatasource;
        case "start":
          return SegmentRecord::getStart;
        case "end":
          return SegmentRecord::getEnd;
        case "size":
          return SegmentRecord::getSize;
        case "version":
          return SegmentRecord::getVersion;
        case "partition_num":
          return SegmentRecord::getPartitionNum;
        case "num_replicas":
          return SegmentRecord::getNumReplicas;
        case "num_rows":
          return SegmentRecord::getNumRows;
        case "is_published":
          return SegmentRecord::isPublished;
        case "is_available":
          return SegmentRecord::isAvailable;
        case "is_realtime":
          return SegmentRecord::isRealtime;
        case "is_overshadowed":
          return SegmentRecord::isOvershadowed;
        case "shard_spec":
          return SegmentRecord::getShardSpec;
        case "dimensions":
          return SegmentRecord::getDimensions;
        case "metrics":
          return SegmentRecord::getMetrics;
        case "last_compaction_state":
          return SegmentRecord::getLastCompactionState;
        default:
          return (SegmentRecord r) -> null;
      }
    }
    
  }

  private interface SegmentRecord
  {
    SegmentId getSegmentId();
    String getDatasource();
    String getStart();
    String getEnd();
    long getSize();
    String getVersion();
    long getPartitionNum();
    long getNumReplicas();
    long getNumRows();
    long isPublished();
    long isAvailable();
    long isRealtime();
    long isOvershadowed();
    String getShardSpec();
    String getDimensions();
    String getMetrics();
    String getLastCompactionState();
  }

  private class PublishedSegmentRecord implements SegmentRecord
  {
    final SegmentWithOvershadowedStatus segment;
    final AvailableSegmentMetadata availableMetadata;

    PublishedSegmentRecord(SegmentWithOvershadowedStatus segment, AvailableSegmentMetadata availableMetadata)
    {
      this.segment = segment;
      this.availableMetadata = availableMetadata;
    }

    @Override
    public SegmentId getSegmentId()
    {
      return segment.getDataSegment().getId();
    }

    @Override
    public String getDatasource()
    {
      return segment.getDataSegment().getDataSource();
    }

    @Override
    public String getStart()
    {
      return segment.getDataSegment().getInterval().getStart().toString();
    }

    @Override
    public String getEnd()
    {
      return segment.getDataSegment().getInterval().getEnd().toString();
    }

    @Override
    public long getSize()
    {
      return segment.getDataSegment().getSize();
    }

    @Override
    public String getVersion()
    {
      return segment.getDataSegment().getVersion();
    }

    @Override
    public long getPartitionNum()
    {
      return segment.getDataSegment().getShardSpec().getPartitionNum();
    }

    @Override
    public long getNumReplicas()
    {
      return (availableMetadata == null) ? 0L : availableMetadata.getNumReplicas();
    }

    @Override
    public long getNumRows()
    {
      return (availableMetadata == null) ? 0L : availableMetadata.getNumRows();
    }

    @Override
    public long isPublished()
    {
      return IS_PUBLISHED_TRUE;
    }

    @Override
    public long isAvailable()
    {
      return IS_AVAILABLE_TRUE;
    }

    @Override
    public long isRealtime()
    {
      return (availableMetadata == null) ? 0L : availableMetadata.isRealtime();
    }

    @Override
    public long isOvershadowed()
    {
      return segment.isOvershadowed() ? IS_OVERSHADOWED_TRUE : IS_OVERSHADOWED_FALSE;
    }

    @Override
    public String getShardSpec()
    {
      return toJsonString(segment.getDataSegment().getShardSpec());
    }

    @Override
    public String getDimensions()
    {
      return toJsonString(segment.getDataSegment().getDimensions());
    }

    @Override
    public String getMetrics()
    {
      return toJsonString(segment.getDataSegment().getMetrics());
    }

    @Override
    public String getLastCompactionState()
    {
      return toJsonString(segment.getDataSegment().getLastCompactionState());
    }

  }

  String toJsonString(Object o)
  {
    if (o == null) {
      return null;
    }

    try {
      return jsonMapper.writeValueAsString(o);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private class AvailableSegmentRecord implements SegmentRecord
  {
    final SegmentId id;
    final AvailableSegmentMetadata data;

    AvailableSegmentRecord(Map.Entry<SegmentId, AvailableSegmentMetadata> entry)
    {
      this.id = entry.getKey();
      this.data = entry.getValue();
    }

    @Override
    public SegmentId getSegmentId()
    {
      return id;
    }

    @Override
    public String getDatasource()
    {
      return id.getDataSource();
    }

    @Override
    public String getStart()
    {
      return id.getInterval().getStart().toString();
    }

    @Override
    public String getEnd()
    {
      return id.getInterval().getStart().toString();
    }

    @Override
    public long getSize()
    {
      return data.getSegment().getSize();
    }

    @Override
    public String getVersion()
    {
      return id.getVersion();
    }

    @Override
    public long getPartitionNum()
    {
      return data.getSegment().getShardSpec().getPartitionNum();
    }

    @Override
    public long getNumReplicas()
    {
      return data.getNumReplicas();
    }

    @Override
    public long getNumRows()
    {
      return data.getNumRows();
    }

    @Override
    public long isPublished()
    {
      // is_published false for unpublished segments
      return IS_PUBLISHED_FALSE;
    }

    @Override
    public long isAvailable()
    {
      // is_available is assumed to be always true for segments announced by historicals or realtime tasks
      return IS_AVAILABLE_TRUE;
    }

    @Override
    public long isRealtime()
    {
      return data.isRealtime();
    }

    @Override
    public long isOvershadowed()
    {
      // there is an assumption here that unpublished segments are never overshadowed
      return IS_OVERSHADOWED_FALSE;
    }

    @Override
    public String getShardSpec()
    {
      return toJsonString(data.getSegment().getShardSpec());
    }

    @Override
    public String getDimensions()
    {
      return toJsonString(data.getSegment().getDimensions());
    }

    @Override
    public String getMetrics()
    {
      return toJsonString(data.getSegment().getMetrics());
    }

    @Override
    public String getLastCompactionState()
    {
      // unpublished segments from realtime tasks will not be compacted yet
      return null;
    }
  }

  private class SegmentsCursorFactory implements SortedCursorFactory
  {
    final Predicate<SegmentRecord> authorizedFilter;

    public SegmentsCursorFactory(AuthenticationResult authenticationResult)
    {
      Preconditions.checkNotNull(authenticationResult, "authenticationResult");
      authorizedFilter = AuthorizationUtils.createPredicateForAuthorizedResources(
          authenticationResult,
          SegmentsTable::resourceActionGenerator,
          authorizerMapper);
    }

    @Override
    public Sequence<Cursor> makeCursors(List<String> columns, Filter filter, Interval interval, VirtualColumns virtualColumns,
            Granularity gran, QueryMetrics<?> queryMetrics, long offset, long limit, List<OrderByColumnSpec> orderBy)
    {
      final Map<SegmentId, AvailableSegmentMetadata> availableSegmentsMap = druidSchema.getSegmentMetadataSnapshot();
      final List<SegmentRecord> publishedSegments = getPublishedSegments(availableSegmentsMap);

      final Set<SegmentId> publishedSegmentIds = publishedSegments.stream()
          .filter(authorizedFilter)
          .map(SegmentRecord::getSegmentId)
          .collect(Collectors.toCollection(HashSet::new));

      final List<SegmentRecord> availableSegments = availableSegmentsMap.entrySet().stream()
          .map(AvailableSegmentRecord::new)
          .filter((SegmentRecord sr) -> !publishedSegmentIds.contains(sr.getSegmentId()))
          .collect(Collectors.toList());

      final Stream<SegmentRecord> records = Streams.concat(availableSegments.stream(), publishedSegments.stream());
      final RowAdapter<SegmentRecord> rowAdapter = new SegmentRecordRowAdapter();
      final List<SegmentRecord> results = records
          .filter(authorizedFilter)
          .filter(new FilterPredicate<>(getRowSignature(), rowAdapter, filter, virtualColumns))
          .sorted(new RowOrderByComparator<>(orderBy, rowAdapter))
          .skip(offset)
          .limit(limit)
          .collect(Collectors.toCollection(ArrayList::new));

      log.info("makeCursors(): results size = %d", results.size());
      for (SegmentRecord sr : results) {
        log.info("makeCursors(): result => " + sr);
      }

      final SettableSupplier<SegmentRecord> rowSupplier = new SettableSupplier<>();
      final RowBasedColumnSelectorFactory<SegmentRecord> csf = RowBasedColumnSelectorFactory.create(
          rowAdapter,
          rowSupplier::get,
          rowSignature,
          true);

      final ListBasedCursor<SegmentRecord> cursor = new ListBasedCursor<>(results, rowSupplier, csf, gran, interval);
      return Sequences.simple(Collections.singletonList(cursor));
    }
  }

  private List<SegmentRecord> getPublishedSegments(Map<SegmentId, AvailableSegmentMetadata> availableSegments)
  {
    List<SegmentRecord> segments = new ArrayList<>();

    Iterator<SegmentWithOvershadowedStatus> iterator = metadataView.getPublishedSegments();
    while (iterator.hasNext()) {
      SegmentWithOvershadowedStatus segment = iterator.next();
      AvailableSegmentMetadata existing = availableSegments.get(segment.getDataSegment().getId());
      segments.add(new PublishedSegmentRecord(segment, existing));
    }

    if (iterator instanceof JsonParserIterator) {
      closeOrThrow((JsonParserIterator<SegmentWithOvershadowedStatus>) iterator);
    }

    return segments;
  }
}
