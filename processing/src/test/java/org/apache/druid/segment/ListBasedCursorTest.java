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

package org.apache.druid.segment;

import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
public class ListBasedCursorTest
{
  final ColumnSelectorFactory csf = Mockito.mock(ColumnSelectorFactory.class);

  @Test
  public void testEmptyList()
  {
    List<Object> underlying = Collections.emptyList();
    SettableSupplier<Object> supplier = new SettableSupplier<>();

    ListBasedCursor<Object> cursor = new ListBasedCursor<>(
        underlying,
        supplier,
        csf,
        Granularities.ALL,
        Intervals.ETERNITY);
    
    Assert.assertNull(supplier.get());
    Assert.assertTrue(cursor.isDone());

    cursor.advance();
    Assert.assertTrue(cursor.isDone());
  }

  @Test
  public void testSingleElementList()
  {
    Object o1 = new Object();
    List<Object> underlying = Collections.singletonList(o1);
    SettableSupplier<Object> supplier = new SettableSupplier<>();

    ListBasedCursor<Object> cursor = new ListBasedCursor<>(
        underlying,
        supplier,
        csf,
        Granularities.ALL,
        Intervals.ETERNITY);

    Assert.assertEquals(o1, supplier.get());
    Assert.assertFalse(cursor.isDone());

    cursor.advance();

    Assert.assertNull(supplier.get());
    Assert.assertTrue(cursor.isDone());
  }

  @Test
  public void testMultiElementList()
  {
    Object o1 = new Object();
    Object o2 = new Object();
    Object o3 = new Object();
    List<Object> underlying = Arrays.asList(o1, o2, o3);

    SettableSupplier<Object> supplier = new SettableSupplier<>();

    ListBasedCursor<Object> cursor = new ListBasedCursor<>(
        underlying,
        supplier,
        csf,
        Granularities.ALL,
        Intervals.ETERNITY);

    Assert.assertEquals(o1, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertEquals(o2, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertEquals(o3, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertNull(supplier.get());
    Assert.assertTrue(cursor.isDone());
  }

  @Test
  public void testReset()
  {
    Object o1 = new Object();
    Object o2 = new Object();
    Object o3 = new Object();
    List<Object> underlying = Arrays.asList(o1, o2, o3);

    SettableSupplier<Object> supplier = new SettableSupplier<>();

    ListBasedCursor<Object> cursor = new ListBasedCursor<>(
        underlying,
        supplier,
        csf,
        Granularities.ALL,
        Intervals.ETERNITY);

    Assert.assertEquals(o1, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertEquals(o2, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    cursor.reset();

    Assert.assertEquals(o1, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertEquals(o2, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertEquals(o3, supplier.get());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();

    Assert.assertNull(supplier.get());
    Assert.assertTrue(cursor.isDone());
  }
}
