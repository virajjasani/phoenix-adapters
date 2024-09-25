import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class DeleteTableIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

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
    public void deleteTableTestWithDeleteTableRequest() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.B, "PK2", ScalarAttributeType.S);
        //creating table for aws
        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);
        //creating table for phoenix
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        //delete table request
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest(tableName);

        //delete table for aws
        DeleteTableResult deleteTableResult1 = amazonDynamoDB.deleteTable(deleteTableRequest);
        //delete table for phoenix
        DeleteTableResult deleteTableResult2 = phoenixDBClient.deleteTable(deleteTableRequest);

        LOGGER.info("Delete Table response from DynamoDB: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(deleteTableResult1));
        LOGGER.info("Delete Table response from Phoenix: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(deleteTableResult2));

        TableDescription tableDescription1 = deleteTableResult1.getTableDescription();
        TableDescription tableDescription2 = deleteTableResult2.getTableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);

    }

    @Test(timeout = 120000)
    public void deleteTableTestWithStringName() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.B, "PK2", ScalarAttributeType.S);
        //creating table for aws
        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);
        //creating table for phoenix
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        //delete table for aws
        DeleteTableResult deleteTableResult1 = amazonDynamoDB.deleteTable(tableName);
        //delete table for phoenix
        DeleteTableResult deleteTableResult2 = phoenixDBClient.deleteTable(tableName);

        LOGGER.info("Delete Table response from DynamoDB: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(deleteTableResult1));
        LOGGER.info("Delete Table response from Phoenix: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(deleteTableResult2));

        TableDescription tableDescription1 = deleteTableResult1.getTableDescription();
        TableDescription tableDescription2 = deleteTableResult2.getTableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

}

