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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.client.BrokerInternalQueryConfig;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.TrueFilter;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordinator.BytesAccumulatingResponseHandler;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.util.TestServerInventoryView;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SupervisorsTableTest extends CalciteTestBase
{
  private static final PlannerConfig PLANNER_CONFIG_DEFAULT = new PlannerConfig();

  private static final List<InputRow> ROWS1 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-01", "m1", "1.0", "dim1", "")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-02", "m1", "2.0", "dim1", "10.1")),
      CalciteTests.createRow(ImmutableMap.of("t", "2000-01-03", "m1", "3.0", "dim1", "2"))
  );

  private static final List<InputRow> ROWS2 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "4.0", "dim2", ImmutableList.of("a"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "5.0", "dim2", ImmutableList.of("abc"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-03", "m1", "6.0"))
  );

  private static final List<InputRow> ROWS3 = ImmutableList.of(
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-01", "m1", "7.0", "dim3", ImmutableList.of("x"))),
      CalciteTests.createRow(ImmutableMap.of("t", "2001-01-02", "m1", "8.0", "dim3", ImmutableList.of("xyz")))
  );

  private SpecificSegmentsQuerySegmentWalker walker;
  private DruidLeaderClient client;
  private ObjectMapper mapper;
  private BytesAccumulatingResponseHandler responseHandler;
  private Request request;
  private DruidSchema druidSchema;
  private AuthorizerMapper authMapper;
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Before
  public void setUp() throws Exception
  {
    client = EasyMock.createMock(DruidLeaderClient.class);
    mapper = TestHelper.makeJsonMapper();
    responseHandler = EasyMock.createMockBuilder(BytesAccumulatingResponseHandler.class)
                              .withConstructor()
                              .addMockedMethod(
                                  "handleResponse",
                                  HttpResponse.class,
                                  HttpResponseHandler.TrafficCop.class
                              )
                              .addMockedMethod("getStatus")
                              .createMock();
    request = EasyMock.createMock(Request.class);
    authMapper = createAuthMapper();

    final File tmpDir = temporaryFolder.newFolder();
    final QueryableIndex index1 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "1"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(
                                                          new CountAggregatorFactory("cnt"),
                                                          new DoubleSumAggregatorFactory("m1", "m1"),
                                                          new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
                                                      )
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS1)
                                              .buildMMappedIndex();

    final QueryableIndex index2 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "2"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS2)
                                              .buildMMappedIndex();
    final QueryableIndex index3 = IndexBuilder.create()
                                              .tmpDir(new File(tmpDir, "3"))
                                              .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                              .schema(
                                                  new IncrementalIndexSchema.Builder()
                                                      .withMetrics(new LongSumAggregatorFactory("m1", "m1"))
                                                      .withRollup(false)
                                                      .build()
                                              )
                                              .rows(ROWS3)
                                              .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate)
        .add(segment1, index1)
        .add(segment2, index2)
        .add(segment3, index3);

    druidSchema = new DruidSchema(
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        new TestServerInventoryView(walker.getSegments(), realtimeSegments),
        new SegmentManager(EasyMock.createMock(SegmentLoader.class)),
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        PLANNER_CONFIG_DEFAULT,
        new NoopEscalator(),
        new BrokerInternalQueryConfig()
    );
    druidSchema.start();
    druidSchema.awaitInitialization();
  }

  private final DataSegment segment1 = new DataSegment(
      "test1",
      Intervals.of("2010/2011"),
      "version1",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment2 = new DataSegment(
      "test2",
      Intervals.of("2011/2012"),
      "version2",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment3 = new DataSegment(
      "test3",
      Intervals.of("2012/2013"),
      "version3",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      new NumberedShardSpec(2, 3),
      1,
      100L
  );
  private final DataSegment segment4 = new DataSegment(
      "test4",
      Intervals.of("2014/2015"),
      "version4",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );
  private final DataSegment segment5 = new DataSegment(
      "test5",
      Intervals.of("2015/2016"),
      "version5",
      null,
      ImmutableList.of("dim1", "dim2"),
      ImmutableList.of("met1", "met2"),
      null,
      1,
      100L
  );

  final List<DataSegment> realtimeSegments = ImmutableList.of(segment2, segment4, segment5);

  @Test
  public void testSupervisorTable() throws Exception
  {

    SupervisorsTable supervisorTable = EasyMock.createMockBuilder(SupervisorsTable.class)
                                                            .withConstructor(
                                                                client,
                                                                mapper,
                                                                authMapper
                                                            )
                                                            .createMock();
    EasyMock.replay(supervisorTable);
    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/supervisor?system"))
            .andReturn(request)
            .anyTimes();

    HttpResponse httpResp = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    InputStreamFullResponseHolder responseHolder = new InputStreamFullResponseHolder(httpResp.getStatus(), httpResp);

    EasyMock.expect(client.go(EasyMock.eq(request), EasyMock.anyObject(InputStreamFullResponseHandler.class))).andReturn(responseHolder).once();

    EasyMock.expect(responseHandler.getStatus()).andReturn(httpResp.getStatus().getCode()).anyTimes();
    EasyMock.expect(request.getUrl())
            .andReturn(new URL("http://test-host:1234/druid/indexer/v1/supervisor?system"))
            .anyTimes();

    String json = "[{\n"
                  + "\t\"id\": \"wikipedia\",\n"
                  + "\t\"state\": \"UNHEALTHY_SUPERVISOR\",\n"
                  + "\t\"detailedState\": \"UNABLE_TO_CONNECT_TO_STREAM\",\n"
                  + "\t\"healthy\": false,\n"
                  + "\t\"specString\": \"{\\\"type\\\":\\\"kafka\\\",\\\"dataSchema\\\":{\\\"dataSource\\\":\\\"wikipedia\\\"}"
                  + ",\\\"context\\\":null,\\\"suspended\\\":false}\",\n"
                  + "\t\"type\": \"kafka\",\n"
                  + "\t\"source\": \"wikipedia\",\n"
                  + "\t\"suspended\": false\n"
                  + "}]";

    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    responseHolder.addChunk(bytesToWrite);
    responseHolder.done();

    EasyMock.replay(client, request, responseHandler);
    DataContext dataContext = createDataContext(Users.SUPER);
    AuthenticationResult authenticationResult = (AuthenticationResult) dataContext.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);
    Cursor cursor = supervisorTable.newCursorFactory(authenticationResult).makeCursors(
        Arrays.asList("supervisor_id", "state", "detailed_state", "healthy", "type", "source", "suspended", "spec"),
        null,
        Intervals.ETERNITY,
        VirtualColumns.nullToEmpty(null),
        Granularities.ALL,
        null,
        0,
        Long.MAX_VALUE,
        Collections.emptyList()).toList().get(0);

    ColumnSelectorFactory csf = cursor.getColumnSelectorFactory();

    Assert.assertFalse(cursor.isDone());

    Assert.assertEquals("wikipedia", csf.makeColumnValueSelector("supervisor_id").getObject());
    Assert.assertEquals("UNHEALTHY_SUPERVISOR", csf.makeColumnValueSelector("state").getObject());
    Assert.assertEquals("UNABLE_TO_CONNECT_TO_STREAM", csf.makeColumnValueSelector("detailed_state").getObject());
    Assert.assertEquals(0L, csf.makeColumnValueSelector("healthy").getObject());
    Assert.assertEquals("kafka", csf.makeColumnValueSelector("type").getObject());
    Assert.assertEquals("wikipedia", csf.makeColumnValueSelector("source").getObject());
    Assert.assertEquals(0L, csf.makeColumnValueSelector("suspended").getObject());
    Assert.assertEquals(
        "{\"type\":\"kafka\",\"dataSchema\":{\"dataSource\":\"wikipedia\"},\"context\":null,\"suspended\":false}",
        csf.makeColumnValueSelector("spec").getObject());

    cursor.advance();

    Assert.assertTrue(cursor.isDone());
  }

  @Test
  public void testSupervisorFilter() throws Exception
  {
    SupervisorsTable supervisorTable =
        new SupervisorsTable(client, mapper, createAuthMapper());

    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/supervisor?system"))
            .andReturn(request)
            .anyTimes();

    final String json = "[{\n"
                  + "\t\"id\": \"wikipedia\",\n"
                  + "\t\"state\": \"UNHEALTHY_SUPERVISOR\",\n"
                  + "\t\"detailedState\": \"UNABLE_TO_CONNECT_TO_STREAM\",\n"
                  + "\t\"healthy\": false,\n"
                  + "\t\"specString\": \"{\\\"type\\\":\\\"kafka\\\",\\\"dataSchema\\\":{\\\"dataSource\\\":\\\"wikipedia\\\"}"
                  + ",\\\"context\\\":null,\\\"suspended\\\":false}\",\n"
                  + "\t\"type\": \"kafka\",\n"
                  + "\t\"source\": \"wikipedia\",\n"
                  + "\t\"suspended\": false\n"
                  + "}]";

    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    EasyMock.expect(client.go(EasyMock.eq(request), EasyMock.anyObject()))
            .andReturn(createFullResponseHolder(httpResponse, json))
            .andReturn(createFullResponseHolder(httpResponse, json))
            .andReturn(createFullResponseHolder(httpResponse, json));

    EasyMock.expect(responseHandler.getStatus())
            .andReturn(httpResponse.getStatus().getCode())
            .anyTimes();
    EasyMock.expect(request.getUrl())
            .andReturn(new URL("http://test-host:1234/druid/indexer/v1/supervisor?system"))
            .anyTimes();

    EasyMock.replay(client, request, responseHandler);

    Function<Filter, Cursor> makeCursor = (Filter filter) -> {
      DataContext dataContext = createDataContext(Users.DATASOURCE_WRITE);
      AuthenticationResult authenticationResult = (AuthenticationResult) dataContext.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);
      Cursor cursor = supervisorTable.newCursorFactory(authenticationResult).makeCursors(
          Arrays.asList("supervisor_id", "state", "detailed_state", "healthy", "type", "source", "suspended", "spec"),
          filter,
          Intervals.ETERNITY,
          VirtualColumns.nullToEmpty(null),
          Granularities.ALL,
          null,
          0,
          Long.MAX_VALUE,
          Collections.emptyList()).toList().get(0);
      return cursor;
    };

    Cursor cursor;

    // Verify that no row is returned for false filter
    cursor = makeCursor.apply(FalseFilter.instance());
    Assert.assertTrue(cursor.isDone());

    // Verify that one row is returned for true filter
    cursor = makeCursor.apply(TrueFilter.instance());
    Assert.assertFalse(cursor.isDone());
    cursor.advance();
    Assert.assertTrue(cursor.isDone());
  }

  @Test
  public void testSupervisorTableAuth() throws Exception
  {
    SupervisorsTable supervisorTable =
        new SupervisorsTable(client, mapper, createAuthMapper());

    EasyMock.expect(client.makeRequest(HttpMethod.GET, "/druid/indexer/v1/supervisor?system"))
            .andReturn(request)
            .anyTimes();

    final String json = "[{\n"
                  + "\t\"id\": \"wikipedia\",\n"
                  + "\t\"state\": \"UNHEALTHY_SUPERVISOR\",\n"
                  + "\t\"detailedState\": \"UNABLE_TO_CONNECT_TO_STREAM\",\n"
                  + "\t\"healthy\": false,\n"
                  + "\t\"specString\": \"{\\\"type\\\":\\\"kafka\\\",\\\"dataSchema\\\":{\\\"dataSource\\\":\\\"wikipedia\\\"}"
                  + ",\\\"context\\\":null,\\\"suspended\\\":false}\",\n"
                  + "\t\"type\": \"kafka\",\n"
                  + "\t\"source\": \"wikipedia\",\n"
                  + "\t\"suspended\": false\n"
                  + "}]";

    HttpResponse httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    EasyMock.expect(client.go(EasyMock.eq(request), EasyMock.anyObject()))
            .andReturn(createFullResponseHolder(httpResponse, json))
            .andReturn(createFullResponseHolder(httpResponse, json))
            .andReturn(createFullResponseHolder(httpResponse, json));

    EasyMock.expect(responseHandler.getStatus())
            .andReturn(httpResponse.getStatus().getCode())
            .anyTimes();
    EasyMock.expect(request.getUrl())
            .andReturn(new URL("http://test-host:1234/druid/indexer/v1/supervisor?system"))
            .anyTimes();

    EasyMock.replay(client, request, responseHandler);

    Function<String, Cursor> makeCursor = (String username) -> {
      DataContext dataContext = createDataContext(username);
      AuthenticationResult authenticationResult = (AuthenticationResult) dataContext.get(PlannerContext.DATA_CTX_AUTHENTICATION_RESULT);
      Cursor cursor = supervisorTable.newCursorFactory(authenticationResult).makeCursors(
          Arrays.asList("supervisor_id", "state", "detailed_state", "healthy", "type", "source", "suspended", "spec"),
          null,
          Intervals.ETERNITY,
          VirtualColumns.nullToEmpty(null),
          Granularities.ALL,
          null,
          0,
          Long.MAX_VALUE,
          Collections.emptyList()).toList().get(0);
      return cursor;
    };

    Cursor cursor;

    // Verify that no row is returned for Datasource Read user
    cursor = makeCursor.apply(Users.DATASOURCE_READ);
    Assert.assertTrue(cursor.isDone());

    // Verify that 1 row is returned for Datasource Write user
    cursor = makeCursor.apply(Users.DATASOURCE_WRITE);
    Assert.assertFalse(cursor.isDone());
    cursor.advance();
    Assert.assertTrue(cursor.isDone());

    // Verify that 1 row is returned for Super user
    cursor = makeCursor.apply(Users.SUPER);
    Assert.assertFalse(cursor.isDone());
    cursor.advance();
    Assert.assertTrue(cursor.isDone());
  }

  /**
   * Creates a response holder that contains the given json.
   */
  private InputStreamFullResponseHolder createFullResponseHolder(
      HttpResponse httpResponse,
      String json
  )
  {
    InputStreamFullResponseHolder responseHolder =
        new InputStreamFullResponseHolder(httpResponse.getStatus(), httpResponse);

    byte[] bytesToWrite = json.getBytes(StandardCharsets.UTF_8);
    responseHolder.addChunk(bytesToWrite);
    responseHolder.done();

    return responseHolder;
  }

  /**
   * Creates a DataContext for the given username.
   */
  private DataContext createDataContext(String username)
  {
    return new DataContext()
    {
      @Override
      public SchemaPlus getRootSchema()
      {
        return null;
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return null;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        return null;
      }

      @Override
      public Object get(String authorizerName)
      {
        return CalciteTests.TEST_SUPERUSER_NAME.equals(username)
               ? CalciteTests.SUPER_USER_AUTH_RESULT
               : new AuthenticationResult(username, authorizerName, null, null);
      }
    };
  }

  private AuthorizerMapper createAuthMapper()
  {
    return new AuthorizerMapper(null)
    {
      @Override
      public Authorizer getAuthorizer(String name)
      {
        return (authenticationResult, resource, action) -> {
          final String username = authenticationResult.getIdentity();

          // Allow access to a Datasource if
          // - any user requests Read access
          // - Super User or Datasource Write User requests Write access
          if (resource.getType().equals(ResourceType.DATASOURCE)) {
            return new Access(
                action == Action.READ
                || username.equals(Users.SUPER)
                || username.equals(Users.DATASOURCE_WRITE)
            );
          }

          return new Access(true);
        };
      }
    };
  }

  /**
   * Usernames to be used in tests.
   */
  private static class Users
  {
    private static final String SUPER = CalciteTests.TEST_SUPERUSER_NAME;
    private static final String DATASOURCE_READ = "datasourceRead";
    private static final String DATASOURCE_WRITE = "datasourceWrite";
  }
}
