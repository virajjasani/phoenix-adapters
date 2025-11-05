/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class MiscIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiscIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();
    private static DynamoDbClient phoenixDBClientV2;
    private static DynamoDbStreamsClient phoenixDBStreamsClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 =
                LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().stop();
        if (restServer != null) {
            restServer.stop();
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }


    @Test
    public void testMixWorkflows1() throws InterruptedException {
        String tableName = "test..__--1234TesT4321";

        CreateTableRequest createRequest = CreateTableRequest.builder().tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder().attributeName("workerId")
                        .attributeType(ScalarAttributeType.S).build()).keySchema(
                        KeySchemaElement.builder().attributeName("workerId").keyType(KeyType.HASH)
                                .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                        .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(createRequest);
        phoenixDBClientV2.createTable(createRequest);

        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams =
                phoenixDBStreamsClientV2.listStreams(listStreamsRequest);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn =
                dynamoDbStreamsClient.listStreams(listStreamsRequest).streams().get(0).streamArn();

        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        ScanResponse localScanResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        Assert.assertEquals("Initial scan count should match", localScanResponse.count(),
                phoenixScanResponse.count());
        Assert.assertEquals("Initial scan count 0", new Integer(0), localScanResponse.count());

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("workerId", AttributeValue.builder().s("localhost:4999").build());
        item1.put("lastAliveTime", AttributeValue.builder().n("1750209020569").build());
        item1.put("appStartupTime", AttributeValue.builder().n("1750209013852").build());
        item1.put("expirationTime", AttributeValue.builder().n("1750209140").build());

        PutItemRequest putRequest1 = PutItemRequest.builder().tableName(tableName).item(item1)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(putRequest1);
        phoenixDBClientV2.putItem(putRequest1);

        localScanResponse = dynamoDbClient.scan(scanRequest);
        phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse,
                "Scan after first put");

        List<Long> timestamps =
                Arrays.asList(1750209030572L, 1750209040572L, 1750209050574L, 1750209060571L,
                        1750209070574L, 1750209080572L, 1750209090574L, 1750209100574L);

        for (int i = 0; i < timestamps.size(); i++) {
            Long timestamp = timestamps.get(i);

            Map<String, AttributeValue> updatedItem = new HashMap<>();
            updatedItem.put("workerId", AttributeValue.builder().s("localhost:4999").build());
            updatedItem.put("lastAliveTime",
                    AttributeValue.builder().n(timestamp.toString()).build());
            updatedItem.put("appStartupTime", AttributeValue.builder().n("1750209013852").build());
            updatedItem.put("expirationTime",
                    AttributeValue.builder().n(String.valueOf(timestamp + 120000)).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(updatedItem)
                            .returnConsumedCapacity(
                                    ReturnConsumedCapacity.TOTAL)
                            .build();

            dynamoDbClient.putItem(putRequest);
            phoenixDBClientV2.putItem(putRequest);

            if (i % 2 == 0) {
                localScanResponse = dynamoDbClient.scan(scanRequest);
                phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

                compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse,
                        "Scan at iteration " + i);
            }

            if (i % 3 == 0) {
                ScanRequest paginatedScanRequest = ScanRequest.builder().tableName(tableName)
                        .exclusiveStartKey(Collections.singletonMap("workerId",
                                AttributeValue.builder().s("localhost:4999").build()))
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

                ScanResponse ddbScanResponse = dynamoDbClient.scan(paginatedScanRequest);
                ScanResponse phoenixDdbScanResponse = phoenixDBClientV2.scan(paginatedScanRequest);

                compareScanResponsesSortedByWorkerId(ddbScanResponse, phoenixDdbScanResponse,
                        "Paginated scan at iteration " + i);
            }
        }

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("workerId", AttributeValue.builder().s("localhost:33279").build());
        item2.put("lastAliveTime", AttributeValue.builder().n("42").build());
        item2.put("appStartupTime", AttributeValue.builder().n("42").build());
        item2.put("expirationTime", AttributeValue.builder().n("120").build());

        PutItemRequest putRequest2 = PutItemRequest.builder().tableName(tableName).item(item2)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(putRequest2);
        phoenixDBClientV2.putItem(putRequest2);

        localScanResponse = dynamoDbClient.scan(scanRequest);
        phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse, "Final scan");

        Map<String, AttributeValue> updateItem = new HashMap<>();
        updateItem.put("workerId", AttributeValue.builder().s("localhost:4999").build());
        updateItem.put("lastAliveTime", AttributeValue.builder().n("1636629071000").build());
        updateItem.put("appStartupTime", AttributeValue.builder().n("1636629071000").build());
        updateItem.put("expirationTime", AttributeValue.builder().n("1636629191").build());

        PutItemRequest updatePutRequest =
                PutItemRequest.builder().tableName(tableName).item(updateItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(updatePutRequest);
        phoenixDBClientV2.putItem(updatePutRequest);

        Map<String, AttributeValue> updateKey = Collections.singletonMap("workerId",
                AttributeValue.builder().s("localhost:4999").build());

        UpdateItemRequest updateRequest =
                UpdateItemRequest.builder().tableName(tableName).key(updateKey)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("REMOVE #0")
                        .expressionAttributeNames(Collections.singletonMap("#0", "appStartupTime"))
                        .build();

        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        GetItemRequest updateGetRequest =
                GetItemRequest.builder().tableName(tableName).key(updateKey).build();

        Map<String, AttributeValue> localUpdatedItem =
                dynamoDbClient.getItem(updateGetRequest).item();
        Map<String, AttributeValue> phoenixUpdatedItem =
                phoenixDBClientV2.getItem(updateGetRequest).item();

        Assert.assertTrue("Items should be equal after REMOVE update",
                ItemComparator.areItemsEqual(localUpdatedItem, phoenixUpdatedItem));
        Assert.assertFalse("appStartupTime should be removed from local item",
                localUpdatedItem.containsKey("appStartupTime"));
        Assert.assertFalse("appStartupTime should be removed from phoenix item",
                phoenixUpdatedItem.containsKey("appStartupTime"));

        DescribeTimeToLiveRequest ttlRequest =
                DescribeTimeToLiveRequest.builder().tableName(tableName).build();

        DescribeTimeToLiveResponse localTtlResponse = dynamoDbClient.describeTimeToLive(ttlRequest);
        DescribeTimeToLiveResponse phoenixTtlResponse =
                phoenixDBClientV2.describeTimeToLive(ttlRequest);

        Assert.assertEquals("TTL status should match",
                localTtlResponse.timeToLiveDescription().timeToLiveStatus(),
                phoenixTtlResponse.timeToLiveDescription().timeToLiveStatus());

        ListTablesRequest listRequest = ListTablesRequest.builder().build();

        ListTablesResponse localListResponse = dynamoDbClient.listTables(listRequest);
        ListTablesResponse phoenixListResponse = phoenixDBClientV2.listTables(listRequest);

        List<String> localTestTables =
                localListResponse.tableNames().stream().filter(name -> name.startsWith("test."))
                        .sorted().collect(Collectors.toList());
        List<String> phoenixTestTables =
                phoenixListResponse.tableNames().stream().filter(name -> name.startsWith("test."))
                        .sorted().collect(Collectors.toList());

        Assert.assertEquals("Test table count should match", localTestTables.size(),
                phoenixTestTables.size());
        Assert.assertEquals("Test table names should match", localTestTables, phoenixTestTables);

        ListTablesRequest paginatedListRequest =
                ListTablesRequest.builder().exclusiveStartTableName(tableName).limit(100).build();

        ListTablesResponse localPaginatedResponse = dynamoDbClient.listTables(paginatedListRequest);
        ListTablesResponse phoenixPaginatedResponse =
                phoenixDBClientV2.listTables(paginatedListRequest);

        List<String> localPaginatedTestTables = localPaginatedResponse.tableNames().stream()
                .filter(name -> name.startsWith("test.")).sorted().collect(Collectors.toList());
        List<String> phoenixPaginatedTestTables = phoenixPaginatedResponse.tableNames().stream()
                .filter(name -> name.startsWith("test.")).sorted().collect(Collectors.toList());

        Assert.assertEquals("Paginated test table count should match",
                localPaginatedTestTables.size(), phoenixPaginatedTestTables.size());
        Assert.assertEquals("Paginated test table names should match", localPaginatedTestTables,
                phoenixPaginatedTestTables);

        DescribeStreamRequest describeStreamRequest =
                DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc =
                phoenixDBStreamsClientV2.describeStream(describeStreamRequest).streamDescription();
        String phoenixShardId = phoenixStreamDesc.shards().get(0).shardId();

        describeStreamRequest = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc =
                dynamoDbStreamsClient.describeStream(describeStreamRequest).streamDescription();
        String dynamoShardId = dynamoStreamDesc.shards().get(0).shardId();

        GetShardIteratorRequest phoenixShardIteratorRequest =
                GetShardIteratorRequest.builder().streamArn(phoenixStreamArn)
                        .shardId(phoenixShardId).shardIteratorType(TRIM_HORIZON).build();
        String phoenixShardIterator =
                phoenixDBStreamsClientV2.getShardIterator(phoenixShardIteratorRequest)
                        .shardIterator();

        GetShardIteratorRequest dynamoShardIteratorRequest =
                GetShardIteratorRequest.builder().streamArn(dynamoStreamArn).shardId(dynamoShardId)
                        .shardIteratorType(TRIM_HORIZON).build();
        String dynamoShardIterator =
                dynamoDbStreamsClient.getShardIterator(dynamoShardIteratorRequest).shardIterator();

        List<Record> allPhoenixRecords = new ArrayList<>();
        List<Record> allDynamoRecords = new ArrayList<>();

        GetRecordsResponse phoenixRecordsResponse;
        GetRecordsResponse dynamoRecordsResponse;

        do {
            GetRecordsRequest phoenixRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(5)
                            .build();
            phoenixRecordsResponse = phoenixDBStreamsClientV2.getRecords(phoenixRecordsRequest);
            allPhoenixRecords.addAll(phoenixRecordsResponse.records());
            phoenixShardIterator = phoenixRecordsResponse.nextShardIterator();
            LOGGER.info("Phoenix shard iterator: {}", phoenixShardIterator);
        } while (phoenixShardIterator != null && !phoenixRecordsResponse.records().isEmpty());

        do {
            GetRecordsRequest dynamoRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(dynamoShardIterator).limit(6).build();
            dynamoRecordsResponse = dynamoDbStreamsClient.getRecords(dynamoRecordsRequest);
            allDynamoRecords.addAll(dynamoRecordsResponse.records());
            dynamoShardIterator = dynamoRecordsResponse.nextShardIterator();
            LOGGER.info("Dynamo shard iterator: {}", dynamoShardIterator);
        } while (dynamoShardIterator != null && !dynamoRecordsResponse.records().isEmpty());

        LOGGER.info("Phoenix stream records count: {}", allPhoenixRecords.size());
        LOGGER.info("DynamoDB stream records count: {}", allDynamoRecords.size());

        TestUtils.validateRecords(allPhoenixRecords, allDynamoRecords);

        DeleteTableRequest deleteRequest =
                DeleteTableRequest.builder().tableName(tableName).build();
        dynamoDbClient.deleteTable(deleteRequest);
        phoenixDBClientV2.deleteTable(deleteRequest);
    }


    private void compareScanResponsesSortedByWorkerId(ScanResponse localScanResponse,
            ScanResponse phoenixScanResponse, String contextMessage) {
        Assert.assertEquals(contextMessage + " - scan count should match",
                localScanResponse.count(), phoenixScanResponse.count());

        List<Map<String, AttributeValue>> localSortedItems = localScanResponse.items().stream()
                .sorted(Comparator.comparing(a -> a.get("workerId").s()))
                .collect(Collectors.toList());
        List<Map<String, AttributeValue>> phoenixSortedItems = phoenixScanResponse.items().stream()
                .sorted(Comparator.comparing(a -> a.get("workerId").s()))
                .collect(Collectors.toList());

        for (int i = 0; i < localSortedItems.size(); i++) {
            Assert.assertTrue(contextMessage + " - items should be equal at index " + i + ": "
                            + localSortedItems.get(i) + " vs " + phoenixSortedItems.get(i),
                    ItemComparator.areItemsEqual(localSortedItems.get(i),
                            phoenixSortedItems.get(i)));
        }
    }

}
