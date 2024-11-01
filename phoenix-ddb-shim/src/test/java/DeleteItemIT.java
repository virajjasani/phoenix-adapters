import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for DeleteItem API. Brings up local DynamoDB server and HBase miniCluster, and tests
 * DeleteItem API with same request against both DDB and HBase/Phoenix servers and
 * compares the response.
 */
public class DeleteItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItemIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws IOException, SQLException {
        LocalDynamoDbTestBase.localDynamoDb().stop();
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

    @Test(timeout = 120000)
    public void testWithOnlyPartitionKey() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //trying to get the same key. we will see returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr2 = "LastPostDateTime, Tags";
        gI.setProjectionExpression(projectionExpr2);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());
    }

    @Test(timeout = 120000)
    public void testWithBothPartitionAndSortKey() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("SubjectNumber", new AttributeValue().withN("20"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //trying to get the same key. we will see returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());

    }

    @Test(timeout = 120000)
    public void testSortKeyNotFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //partition key exists but sort key does not
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("SubjectNumber", new AttributeValue().withN("25"));
        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }
    }

    @Test(timeout = 120000)
    public void testBothKeysNotFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //both keys do not exist in the table
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        key.put("SubjectNumber", new AttributeValue().withN("25"));

        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }

    }
    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        item.put("LastPostedBy", new AttributeValue().withS("brad@example.com"));
        item.put("LastPostDateTime", new AttributeValue().withS("201303201042"));
        item.put("Tags", new AttributeValue().withS(("Update")));
        return item;
    }
    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("SubjectNumber", new AttributeValue().withN("20"));
        item.put("LastPostedBy", new AttributeValue().withS("fred@example.com"));
        item.put("LastPostDateTime", new AttributeValue().withS("201303201023"));
        item.put("Tags", new AttributeValue().withS(("Update,Multiple Items,HelpMe")));
        item.put("Subject", new AttributeValue().withS(("How do I update multiple items?")));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

}