package org.apache.phoenix.ddb.utils;

import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Utility methods to implement DynamoDB Streams abstractions.
 * See <a href="https://salesforce.quip.com/vunDA0Fwedt5">DynamoDB Streams using Phoenix CDC</a>
 */
public class DDBShimCDCUtils {

    /**
     * Support these many different change records at the same timestamp with unique sequence number.
     */
    public static final int MAX_NUM_CHANGES_AT_TIMESTAMP = 100000;

    private static final String STREAM_NAME_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

    private static final String STREAM_STATUS_QUERY
            = "SELECT STREAM_STATUS FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s'";

    /**
     * Return the KeySchema for the given PTable.
     */
    public static List<KeySchemaElement> getKeySchema(PTable table) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        List<PColumn> pkCols = table.getPKColumns();
        keySchema.add(new KeySchemaElement(pkCols.get(0).getName().getString(), KeyType.HASH));
        if (pkCols.size() == 2) {
            keySchema.add(new KeySchemaElement(pkCols.get(1).getName().getString(), KeyType.RANGE));
        }
        return keySchema;
    }

    /**
     * Get the STREAM_STATUS from SYSTEM.CDC_STREAM_STATUS for the given tableName and streamName.
     */
    public static String getStreamStatus(Connection conn, String tableName, String streamName)
            throws SQLException {
        String query = String.format(STREAM_STATUS_QUERY, tableName, streamName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            throw new SQLException("No stream was found with streamName = " + streamName);
        }
    }

    /**
     * Get the STREAM_NAME in ENABLED/ENABLING status from SYSTEM.CDC_STREAM_STATUS for the given tableName.
     */
    public static String getEnabledStreamName(Connection conn, String tableName) throws SQLException {
        String query = String.format(STREAM_NAME_QUERY, tableName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    /**
     * Return human-readable format for index creation timestamp as Stream Label.
     */
    public static String getStreamLabel(long timestamp) {
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return format.format(date);
    }

    /**
     * Parse tableName from streamName.
     */
    public static String getTableNameFromStreamName(String streamName) {
        // phoenix-cdc-stream-{tableName}-{cdc object name}-{cdc index timestamp}
        String[] parts = streamName.split("-");
        if (parts.length != 6) {
            throw new IllegalArgumentException("Stream Name format is not correct.");
        }
        return parts[3];
    }

    /**
     * Parse CDC index creation time from streamName.
     */
    public static long getCDCIndexTimestampFromStreamName(String streamName) {
        // phoenix-cdc-stream-{tableName}-{cdc object name}-{cdc index timestamp}
        return Long.parseLong(streamName.substring(streamName.lastIndexOf("-")+1));
    }
}
