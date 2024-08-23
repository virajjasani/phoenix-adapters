import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.util.PhoenixRuntime;
import org.bson.BsonDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class PutItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @AfterClass
    public static void stopLocalDynamoDb() {
        LocalDynamoDbTestBase.localDynamoDb().stop();
    }

    @Test
    public void putItemHashKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        phoenixDBClient.putItem(putItemRequest);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").getS());
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(2);
            Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, item2);
        }
    }

    @Test
    public void putItemHashRangeKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        phoenixDBClient.putItem(putItemRequest);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").getS());
            Assert.assertEquals(rs.getDouble(2), Double.parseDouble(item.get("attr_1").getN()), 0.0);
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(3);
            Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, item2);
        }
    }

    @Test
    public void putItemWithGlobalIndexWithHashKeyTest() throws Exception {
        // create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // add index on Title
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "Title",
                ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        phoenixDBClient.putItem(putItemRequest);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").getS());
            Assert.assertEquals(rs.getDouble(2), Double.parseDouble(item.get("attr_1").getN()), 0.0);
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(3);
            Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, item2);

            // check index row (Title, attr_0, attr1, COL)
            rs = connection.createStatement().executeQuery("SELECT * FROM G_IDX_" + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("Title").getS());
            Assert.assertEquals(rs.getString(2), item.get("attr_0").getS());
            Assert.assertEquals(rs.getDouble(3), Double.parseDouble(item.get("attr_1").getN()), 0.0);
            bsonDoc = (BsonDocument) rs.getObject(4);
            Map<String, AttributeValue> item3 = BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, item3);
        }
    }

    @Test
    public void putItemIndexSortedTest() throws Exception {
        // create table [attr_0, idx_attr]
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // add index on Title
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "idx_attr",
                ScalarAttributeType.N, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);

        // put some items
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val1", "123")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val2", "123.0001")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val3", "-123.01")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val4", "-123")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val5", "122.999")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val6", "-122.9999")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val7", "122.9999")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val8", "0")));
        phoenixDBClient.putItem(new PutItemRequest(tableName, getItemForIndexSortingTest("val9", "0.123")));

        // check index rows are sorted
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM G_IDX_" + tableName);
            Assert.assertTrue(rs.next());
            Double val = rs.getDouble(1);
            while (rs.next()) {
                Assert.assertTrue(rs.getDouble(1) > val);
                val = rs.getDouble(1);
            }
        }
    }

    private Map<String, AttributeValue> getItemForIndexSortingTest(String k, String v) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS(k));
        item.put("idx_attr", new AttributeValue().withN(v));
        return item;
    }
}
