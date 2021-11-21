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

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.query.VirtualDataSource;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec.Direction;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.SortedCursorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.BooleanValueMatcher;
import org.apache.druid.server.security.AuthenticationResult;

import java.io.Closeable;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class VirtualTable extends AbstractTable implements TranslatableTable
{
  protected final RowSignature rowSignature;
  protected final String tableName;

  public VirtualTable(RowSignature rowSignature, String tableName)
  {
    this.rowSignature = Preconditions.checkNotNull(rowSignature, "rowSignature");
    this.tableName = Preconditions.checkNotNull(tableName, "tableName");
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable)
  {
    return LogicalTableScan.create(context.getCluster(), relOptTable);
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.SYSTEM_TABLE;
  }

  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  public String getTableName()
  {
    return tableName;
  }
  
  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return RowSignatures.toRelDataType(getRowSignature(), typeFactory);
  }

  public VirtualDataSource getDataSource(AuthenticationResult authenticationResult)
  {
    return VirtualDataSource.fromVirtualTable(
        newCursorFactory(authenticationResult),
        getRowSignature(),
        tableName);
  }

  public abstract SortedCursorFactory newCursorFactory(AuthenticationResult authenticationResult);

  public static class FilterPredicate<RowType> implements Predicate<RowType>
  {
    final SettableSupplier<RowType> rowSupplier = new SettableSupplier<>();
    final ValueMatcher matcher;

    public FilterPredicate(
        RowSignature rowSignature,
        RowAdapter<RowType> rowAdapter,
        Filter filter,
        VirtualColumns virtualColumns
    )
    {
      if (filter == null) {
        matcher = BooleanValueMatcher.of(true);
      } else {
        final RowBasedColumnSelectorFactory<RowType> csf = RowBasedColumnSelectorFactory.create(
            rowAdapter,
            rowSupplier::get,
            rowSignature,
            true);
        final ColumnSelectorFactory vcsf = virtualColumns.wrap(csf);
        matcher = filter.makeMatcher(vcsf);
      }
    }

    @Override
    public boolean test(RowType row)
    {
      rowSupplier.set(row);
      return matcher.matches();
    }
  }

  public static void closeOrThrow(Closeable c)
  {
    if (c != null) {
      try {
        c.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class RowOrderByComparator<RowType> implements Comparator<RowType>
  {
    final List<OrderByColumnSpec> orderBys;
    final RowAdapter<RowType> rowAdapter;

    public RowOrderByComparator(List<OrderByColumnSpec> orderBys, RowAdapter<RowType> rowAdapter)
    {
      this.orderBys = orderBys;
      this.rowAdapter = rowAdapter;
    }

    @Override
    public int compare(RowType arg0, RowType arg1)
    {
      for (OrderByColumnSpec orderBy : orderBys) {
        Function<RowType, Object> fn = rowAdapter.columnFunction(orderBy.getDimension());

        Comparable val1 = (Comparable) fn.apply(arg0);
        Comparable val2 = (Comparable) fn.apply(arg1);

        final int comparison;
        if (orderBy.getDirection() == Direction.ASCENDING) {
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

}
