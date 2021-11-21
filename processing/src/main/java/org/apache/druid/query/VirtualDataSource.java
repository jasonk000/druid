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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.SortedCursorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Represents an virtual datasource, where the rows are backed by a source only instantiated at runtime.
 * 
 * This provides a similar function to the {@see InlineDataSource}, and is separate to support
 * better push-down into ScanQuery.
 */
public class VirtualDataSource implements DataSource
{
  private final SortedCursorFactory cursorFactory;
  private final RowSignature signature;
  private final Set<String> tableNames;

  private VirtualDataSource(
      final SortedCursorFactory cursorFactory,
      final RowSignature signature,
      final String tableName
  )
  {
    this.cursorFactory = Preconditions.checkNotNull(cursorFactory, "'cursorFactory' must be nonnull");
    this.signature = Preconditions.checkNotNull(signature, "'signature' must be nonnull");
    Preconditions.checkNotNull(tableName, "'tableName' must be nonnull");
    tableNames = Collections.singleton(tableName);
  }

  public static VirtualDataSource fromVirtualTable(
      final SortedCursorFactory cursorFactory,
      final RowSignature signature,
      final String tableName
  )
  {
    return new VirtualDataSource(cursorFactory, signature, tableName);
  }

  public SortedCursorFactory getCursorFactory()
  {
    return cursorFactory;
  }

  public boolean canScanOrdered(long offset, long limit, List<OrderByColumnSpec> orderBy)
  {
    return true;
  }

  @Override
  public Set<String> getTableNames()
  {
    return tableNames;
  }

  @JsonProperty
  public List<String> getColumnNames()
  {
    return signature.getColumnNames();
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<ColumnType> getColumnTypes()
  {
    if (IntStream.range(0, signature.size()).noneMatch(i -> signature.getColumnType(i).isPresent())) {
      // All types are null; return null for columnTypes so it doesn't show up in serialized JSON.
      return null;
    } else {
      return IntStream.range(0, signature.size())
                      .mapToObj(i -> signature.getColumnType(i).orElse(null))
                      .collect(Collectors.toList());
    }
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return true;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  /**
   * Returns the row signature (map of column name to type) for this datasource. Note that types may
   * be null, meaning we know we have a column with a certain name, but we don't know what its type is.
   */
  public RowSignature getRowSignature()
  {
    return signature;
  }

  public RowAdapter<Object[]> rowAdapter()
  {
    return new RowAdapter<Object[]>()
    {
      @Override
      public ToLongFunction<Object[]> timestampFunction()
      {
        final int columnNumber = signature.indexOf(ColumnHolder.TIME_COLUMN_NAME);

        if (columnNumber >= 0) {
          return row -> (long) row[columnNumber];
        } else {
          return row -> 0L;
        }
      }

      @Override
      public Function<Object[], Object> columnFunction(String columnName)
      {
        final int columnNumber = signature.indexOf(columnName);

        if (columnNumber >= 0) {
          return row -> row[columnNumber];
        } else {
          return row -> null;
        }
      }
    };
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VirtualDataSource that = (VirtualDataSource) o;
    return Objects.equals(cursorFactory, that.cursorFactory) &&
           Objects.equals(signature, that.signature) &&
           Objects.equals(tableNames, that.tableNames);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(cursorFactory, signature, tableNames);
  }

  @Override
  public String toString()
  {
    // Don't include 'rows' in stringification, because it might be long and/or lazy.
    return "VirtualDataSource{" +
           "tableNames=" + tableNames +
           ", signature=" + signature +
           '}';
  }
}
