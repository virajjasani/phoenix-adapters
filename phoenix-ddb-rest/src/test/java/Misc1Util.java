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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class Misc1Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(Misc1Util.class);

    public static void test1(DynamoDbClient dynamoDbClient, DynamoDbClient phoenixDBClientV2,
            DynamoDbStreamsClient dynamoDbStreamsClient,
            DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {
        String leaseTableName = "test.dataLease--12_99";
        String objectsTableName = "test.cloudX9_";

        CreateTableRequest leaseTableRequest =
                CreateTableRequest.builder().tableName(leaseTableName).attributeDefinitions(
                        AttributeDefinition.builder().attributeName("shardKey")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("owner")
                                .attributeType(ScalarAttributeType.S).build()).keySchema(
                        KeySchemaElement.builder().attributeName("shardKey").keyType(KeyType.HASH)
                                .build()).globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder().indexName("ownerIndex").keySchema(
                                        KeySchemaElement.builder().attributeName("owner")
                                                .keyType(KeyType.HASH).build()).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                        .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(leaseTableRequest);
        phoenixDBClientV2.createTable(leaseTableRequest);

        CreateTableRequest objectsTableRequest =
                CreateTableRequest.builder().tableName(objectsTableName).attributeDefinitions(
                                AttributeDefinition.builder().attributeName("0")
                                        .attributeType(ScalarAttributeType.B).build(),
                                AttributeDefinition.builder().attributeName("1")
                                        .attributeType(ScalarAttributeType.B).build()).keySchema(
                                KeySchemaElement.builder().attributeName("0").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("1").keyType(KeyType.RANGE)
                                        .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                        .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                                .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(objectsTableRequest);
        phoenixDBClientV2.createTable(objectsTableRequest);

        UpdateTimeToLiveRequest enableTtlRequest =
                UpdateTimeToLiveRequest.builder().tableName(objectsTableName)
                        .timeToLiveSpecification(
                                TimeToLiveSpecification.builder().attributeName("#").enabled(true)
                                        .build()).build();

        UpdateTimeToLiveResponse dynamoUpdateTtlResponse =
                dynamoDbClient.updateTimeToLive(enableTtlRequest);
        UpdateTimeToLiveResponse phoenixUpdateTtlResponse =
                phoenixDBClientV2.updateTimeToLive(enableTtlRequest);

        Assert.assertEquals("UpdateTimeToLive responses should match",
                dynamoUpdateTtlResponse.timeToLiveSpecification(),
                phoenixUpdateTtlResponse.timeToLiveSpecification());

        String migrationKey = "Migration_test.cloudX9_/2025-06-19T20:19:58.767";
        GetItemRequest migrationGetRequest = GetItemRequest.builder().tableName(leaseTableName)
                .key(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(migrationKey).build())).consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        GetItemResponse dynamoMigrationGet = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Migration GetItem responses should match", dynamoMigrationGet.item(),
                phoenixMigrationGet.item());

        PutItemRequest migrationPutRequest = PutItemRequest.builder().tableName(leaseTableName)
                .item(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(migrationKey).build()))
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(migrationPutRequest);
        phoenixDBClientV2.putItem(migrationPutRequest);

        String leaseKey =
                "test.cloudX9_/2025-06-19T20:19:58.767/d0dba5c1e3e646d04bff003bbdefde0e";
        Map<String, AttributeValue> leaseItem = new HashMap<>();
        leaseItem.put("shardKey", AttributeValue.builder().s(leaseKey).build());
        leaseItem.put("owner", AttributeValue.builder().s("localhost:4999").build());
        leaseItem.put("createdTime", AttributeValue.builder().n("1750364402944").build());

        PutItemRequest leasePutRequest =
                PutItemRequest.builder().tableName(leaseTableName).item(leaseItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .conditionExpression("attribute_not_exists(#0)")
                        .expressionAttributeNames(Collections.singletonMap("#0", "shardKey"))
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        dynamoDbClient.putItem(leasePutRequest);
        phoenixDBClientV2.putItem(leasePutRequest);

        Map<String, AttributeValue> objectItem = new HashMap<>();
        objectItem.put("0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("AAEAIA==")))
                .build());
        objectItem.put("1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("Zm9vAAE=")))
                .build());
        objectItem.put("B", AttributeValue.builder().n("42").build());

        Map<String, AttributeValue> nestedC = new HashMap<>();
        nestedC.put("C", AttributeValue.builder().n("42").build());
        Map<String, AttributeValue> nestedE = new HashMap<>();
        nestedE.put("D", AttributeValue.builder().n("42").build());
        nestedC.put("E", AttributeValue.builder()
                .m(Collections.singletonMap("YmF6AAE", AttributeValue.builder().m(nestedE).build()))
                .build());

        objectItem.put("C", AttributeValue.builder()
                .m(Collections.singletonMap("YmFyAAE", AttributeValue.builder().m(nestedC).build()))
                .build());
        objectItem.put("-", AttributeValue.builder().n("1").build());

        Map<String, String> objectAttrNames = new HashMap<>();
        objectAttrNames.put("#0", "0");
        objectAttrNames.put("#1", "1");
        objectAttrNames.put("#2", "%");
        objectAttrNames.put("#3", "-");

        PutItemRequest objectPutRequest =
                PutItemRequest.builder().tableName(objectsTableName).item(objectItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).conditionExpression(
                                "((attribute_not_exists(#0) AND attribute_not_exists(#1)) OR "
                                        + "attribute_exists(#2)) AND (((attribute_not_exists(#0)"
                                        + " AND attribute_not_exists(#1)) OR attribute_exists(#2))"
                                        + " OR (#3 = :0))")
                        .expressionAttributeNames(objectAttrNames).expressionAttributeValues(
                                Collections.singletonMap(":0", AttributeValue.builder().n("1").build()))
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        dynamoDbClient.putItem(objectPutRequest);
        phoenixDBClientV2.putItem(objectPutRequest);

        Map<String, AttributeValue> objectKey = new HashMap<>();
        objectKey.put("0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("AAEAIA==")))
                .build());
        objectKey.put("1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("Zm9vAAE=")))
                .build());

        GetItemRequest objectGetRequest =
                GetItemRequest.builder().tableName(objectsTableName).key(objectKey)
                        .consistentRead(true).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .build();

        GetItemResponse dynamoObjectGet = dynamoDbClient.getItem(objectGetRequest);
        GetItemResponse phoenixObjectGet = phoenixDBClientV2.getItem(objectGetRequest);

        Assert.assertTrue("Object GetItem responses should be equal",
                ItemComparator.areItemsEqual(dynamoObjectGet.item(), phoenixObjectGet.item()));

        Map<String, String> updateAttrNames = new HashMap<>();
        updateAttrNames.put("#0", "0");
        updateAttrNames.put("#1", "1");
        updateAttrNames.put("#2", "%");
        updateAttrNames.put("#3", "-");
        updateAttrNames.put("#4", "^");
        updateAttrNames.put("#5", ">");
        updateAttrNames.put("#6", "B");
        updateAttrNames.put("#7", "C");

        Map<String, AttributeValue> updateAttrValues = new HashMap<>();
        updateAttrValues.put(":0", AttributeValue.builder().n("1").build());
        updateAttrValues.put(":1", AttributeValue.builder().n("42").build());
        Map<String, AttributeValue> newC = new HashMap<>();
        newC.put("C", AttributeValue.builder().n("42").build());
        newC.put("E", AttributeValue.builder().m(Collections.emptyMap()).build());
        updateAttrValues.put(":2", AttributeValue.builder()
                .m(Collections.singletonMap("YmFyAAE", AttributeValue.builder().m(newC).build()))
                .build());

        UpdateItemRequest objectUpdateRequest =
                UpdateItemRequest.builder().tableName(objectsTableName).key(objectKey)
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #6 = :1, #7 = :2 REMOVE #4, #5").conditionExpression(
                                "((attribute_exists(#0) AND attribute_exists(#1)) AND "
                                        + "attribute_not_exists(#2)) AND (((attribute_exists(#0) "
                                        + "AND attribute_exists(#1)) AND attribute_not_exists(#2))"
                                        + " AND (((attribute_not_exists(#0) AND "
                                        + "attribute_not_exists(#1)) OR attribute_exists(#2)) "
                                        + "OR (#3 = :0)))")
                        .expressionAttributeNames(updateAttrNames)
                        .expressionAttributeValues(updateAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoObjectUpdate = dynamoDbClient.updateItem(objectUpdateRequest);
        UpdateItemResponse phoenixObjectUpdate = phoenixDBClientV2.updateItem(objectUpdateRequest);

        Assert.assertTrue("Object UpdateItem responses should be equal",
                ItemComparator.areItemsEqual(dynamoObjectUpdate.attributes(),
                        phoenixObjectUpdate.attributes()));

        Map<String, String> leaseAttrNames = new HashMap<>();
        leaseAttrNames.put("#0", "shardKey");
        leaseAttrNames.put("#1", "owner");
        leaseAttrNames.put("#2", "checkpoint");
        leaseAttrNames.put("#3", "lastCheckpointTime");

        Map<String, AttributeValue> leaseAttrValues = new HashMap<>();
        leaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        leaseAttrValues.put(":1", AttributeValue.builder().s("000175036441063000000").build());
        leaseAttrValues.put(":2", AttributeValue.builder().n("1750364411612").build());

        UpdateItemRequest leaseUpdateRequest = UpdateItemRequest.builder().tableName(leaseTableName)
                .key(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(leaseKey).build()))
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .updateExpression("SET #2 = :1, #3 = :2").conditionExpression(
                        "attribute_exists(#0) AND #1 = :0 AND attribute_not_exists(#2)")
                .expressionAttributeNames(leaseAttrNames).expressionAttributeValues(leaseAttrValues)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        dynamoDbClient.updateItem(leaseUpdateRequest);
        phoenixDBClientV2.updateItem(leaseUpdateRequest);

        GetItemResponse dynamoMigrationGet2 = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet2 = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Second migration GetItem responses should match",
                dynamoMigrationGet2.item(), phoenixMigrationGet2.item());

        Map<String, String> releaseAttrNames = new HashMap<>();
        releaseAttrNames.put("#0", "shardKey");
        releaseAttrNames.put("#1", "owner");
        releaseAttrNames.put("#2", "checkpoint");
        releaseAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> releaseAttrValues = new HashMap<>();
        releaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        releaseAttrValues.put(":1", AttributeValue.builder().s("000175036441063000000").build());
        releaseAttrValues.put(":2", AttributeValue.builder().n("42").build());

        UpdateItemRequest leaseReleaseRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #3 = :2 REMOVE #1")
                        .conditionExpression("attribute_exists(#0) AND #1 = :0 AND #2 = :1")
                        .expressionAttributeNames(releaseAttrNames)
                        .expressionAttributeValues(releaseAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoReleaseUpdate = dynamoDbClient.updateItem(leaseReleaseRequest);
        UpdateItemResponse phoenixReleaseUpdate = phoenixDBClientV2.updateItem(leaseReleaseRequest);

        Assert.assertTrue("Lease release responses should be equal",
                ItemComparator.areItemsEqual(dynamoReleaseUpdate.attributes(),
                        phoenixReleaseUpdate.attributes()));

        GetItemResponse dynamoMigrationGet3 = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet3 = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Third migration GetItem responses should match",
                dynamoMigrationGet3.item(), phoenixMigrationGet3.item());

        Map<String, String> reacquireAttrNames = new HashMap<>();
        reacquireAttrNames.put("#0", "shardKey");
        reacquireAttrNames.put("#1", "owner");
        reacquireAttrNames.put("#2", "checkpoint");
        reacquireAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> reacquireAttrValues = new HashMap<>();
        reacquireAttrValues.put(":0", AttributeValue.builder().s("000175036441063000000").build());
        reacquireAttrValues.put(":1", AttributeValue.builder().s("localhost:4999").build());
        reacquireAttrValues.put(":2", AttributeValue.builder().n("1750364462264").build());

        UpdateItemRequest leaseReacquireRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #1 = :1, #3 = :2").conditionExpression(
                                "attribute_exists(#0) AND attribute_not_exists(#1) AND #2 = :0")
                        .expressionAttributeNames(reacquireAttrNames)
                        .expressionAttributeValues(reacquireAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoReacquireUpdate = dynamoDbClient.updateItem(leaseReacquireRequest);
        UpdateItemResponse phoenixReacquireUpdate =
                phoenixDBClientV2.updateItem(leaseReacquireRequest);

        Assert.assertTrue("Lease reacquire responses should be equal",
                ItemComparator.areItemsEqual(dynamoReacquireUpdate.attributes(),
                        phoenixReacquireUpdate.attributes()));

        Map<String, String> finalReleaseAttrNames = new HashMap<>();
        finalReleaseAttrNames.put("#0", "shardKey");
        finalReleaseAttrNames.put("#1", "owner");
        finalReleaseAttrNames.put("#2", "checkpoint");
        finalReleaseAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> finalReleaseAttrValues = new HashMap<>();
        finalReleaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        finalReleaseAttrValues.put(":1",
                AttributeValue.builder().s("000175036441063000000").build());
        finalReleaseAttrValues.put(":2", AttributeValue.builder().n("1750364465201").build());

        UpdateItemRequest finalReleaseRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #3 = :2 REMOVE #1")
                        .conditionExpression("attribute_exists(#0) AND #1 = :0 AND #2 = :1")
                        .expressionAttributeNames(finalReleaseAttrNames)
                        .expressionAttributeValues(finalReleaseAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoFinalUpdate = dynamoDbClient.updateItem(finalReleaseRequest);
        UpdateItemResponse phoenixFinalUpdate = phoenixDBClientV2.updateItem(finalReleaseRequest);

        Assert.assertTrue("Final lease release responses should be equal",
                ItemComparator.areItemsEqual(dynamoFinalUpdate.attributes(),
                        phoenixFinalUpdate.attributes()));

        TestUtils.compareAllStreamRecords(leaseTableName, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
        TestUtils.compareAllStreamRecords(objectsTableName, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);

        DeleteTableRequest deleteLeaseTable =
                DeleteTableRequest.builder().tableName(leaseTableName).build();
        DeleteTableRequest deleteObjectsTable =
                DeleteTableRequest.builder().tableName(objectsTableName).build();

        dynamoDbClient.deleteTable(deleteLeaseTable);
        phoenixDBClientV2.deleteTable(deleteLeaseTable);
        dynamoDbClient.deleteTable(deleteObjectsTable);
        phoenixDBClientV2.deleteTable(deleteObjectsTable);
    }

    public static void test2(DynamoDbClient dynamoDbClient, DynamoDbClient phoenixDBClientV2,
            DynamoDbStreamsClient dynamoDbStreamsClient,
            DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {
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

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 60; i++) {
                Map<String, AttributeValue> initialItem = new HashMap<>();
                initialItem.put("workerId",
                        AttributeValue.builder().s(j == 0 ? "batch:delete:" : "batch:worker:" + i)
                                .build());
                initialItem.put("lastAliveTime",
                        AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                .build());
                initialItem.put("appStartupTime",
                        AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                .build());
                initialItem.put("expirationTime", AttributeValue.builder()
                        .n(String.valueOf(System.currentTimeMillis() + 120000)).build());

                PutItemRequest putRequest =
                        PutItemRequest.builder().tableName(tableName).item(initialItem).build();
                dynamoDbClient.putItem(putRequest);
                phoenixDBClientV2.putItem(putRequest);
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

        int totalItems = 120;
        int batchSize = 25;
        int numBatches = (int) Math.ceil((double) totalItems / batchSize);

        for (int batchNum = 0; batchNum < numBatches; batchNum++) {
            List<WriteRequest> writeRequests = new ArrayList<>();

            int startIndex = batchNum * batchSize;
            int endIndex = Math.min(startIndex + batchSize, totalItems);
            int itemsInThisBatch = endIndex - startIndex;

            for (int i = 0; i < itemsInThisBatch; i++) {
                int itemIndex = startIndex + i;

                if (itemIndex % 2 == 0) {
                    Map<String, AttributeValue> batchItem = new HashMap<>();
                    batchItem.put("workerId", AttributeValue.builder()
                            .s("batch:delete:" + (itemIndex + itemsInThisBatch)).build());
                    batchItem.put("lastAliveTime", AttributeValue.builder()
                            .n(String.valueOf(System.currentTimeMillis() + itemIndex)).build());
                    batchItem.put("appStartupTime",
                            AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                    .build());
                    batchItem.put("expirationTime", AttributeValue.builder()
                            .n(String.valueOf(System.currentTimeMillis() + 120000 + itemIndex))
                            .build());

                    writeRequests.add(WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(batchItem).build()).build());
                } else {
                    int deleteIndex = itemIndex / 2;
                    Map<String, AttributeValue> deleteKey = Collections.singletonMap("workerId",
                            AttributeValue.builder().s("batch:delete:" + deleteIndex).build());

                    writeRequests.add(WriteRequest.builder()
                            .deleteRequest(DeleteRequest.builder().key(deleteKey).build()).build());
                }
            }

            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(tableName, writeRequests);

            BatchWriteItemRequest batchWriteRequest =
                    BatchWriteItemRequest.builder().requestItems(requestItems).build();

            BatchWriteItemResponse dynamoBatchResponse =
                    dynamoDbClient.batchWriteItem(batchWriteRequest);
            BatchWriteItemResponse phoenixBatchResponse =
                    phoenixDBClientV2.batchWriteItem(batchWriteRequest);

            Assert.assertEquals("Batch " + batchNum + " unprocessed items should match",
                    dynamoBatchResponse.unprocessedItems().size(),
                    phoenixBatchResponse.unprocessedItems().size());

            if (!dynamoBatchResponse.unprocessedItems().isEmpty()) {
                LOGGER.warn("Batch {} had {} unprocessed items", batchNum,
                        dynamoBatchResponse.unprocessedItems().size());
            }

            LOGGER.info("Completed batch {} with {} write requests (items {}-{})",
                    batchNum, writeRequests.size(), startIndex, endIndex - 1);
        }

        phoenixShardIterator = phoenixDBStreamsClientV2
                .getShardIterator(phoenixShardIteratorRequest).shardIterator();

        dynamoShardIterator = dynamoDbStreamsClient
                .getShardIterator(dynamoShardIteratorRequest).shardIterator();

        allPhoenixRecords = new ArrayList<>();
        allDynamoRecords = new ArrayList<>();

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

        allDynamoRecords.sort(
                Comparator.comparing(record -> record.dynamodb().keys().get("workerId").s()));
        allPhoenixRecords.sort(
                Comparator.comparing(record -> record.dynamodb().keys().get("workerId").s()));

        TestUtils.validateRecords(allPhoenixRecords, allDynamoRecords);

        DeleteTableRequest deleteRequest =
                DeleteTableRequest.builder().tableName(tableName).build();
        dynamoDbClient.deleteTable(deleteRequest);
        phoenixDBClientV2.deleteTable(deleteRequest);
    }

    private static void compareScanResponsesSortedByWorkerId(ScanResponse localScanResponse,
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
