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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.QueryMaker;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.VirtualTable;

/**
 * A {@link RelOptRule} that converts {@link LogicalTableScan} of a {@link VirtualTable} into
 * {@link InlineDataSource}. This rule is used when the query directly reads in-memory data.
 */
public class DruidVirtualTableRule extends RelOptRule
{
  private final QueryMaker queryMaker;

  public DruidVirtualTableRule(QueryMaker queryMaker)
  {
    super(operand(LogicalTableScan.class, any()));
    this.queryMaker = queryMaker;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final LogicalTableScan scan = call.rel(0);
    final RelOptTable table = scan.getTable();

    final VirtualTable virtualTable = table.unwrap(VirtualTable.class);
    if (virtualTable != null) {
      final AuthenticationResult authResult =
          queryMaker.getPlannerContext().getAuthenticationResult();
      final DruidTable druidTable = new DruidTable(
          virtualTable.getDataSource(authResult),
          virtualTable.getRowSignature(),
          false,
          false);
      call.transformTo(
          DruidQueryRel.fullScan(scan, table, druidTable, queryMaker)
      );
    }
  }
}
