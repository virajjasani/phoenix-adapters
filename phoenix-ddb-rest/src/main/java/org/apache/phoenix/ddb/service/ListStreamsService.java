package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.service.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.CDCUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

public class ListStreamsService {

    // For now, we will only return the currently enabled or enabling stream
    // TODO: once phoenix can handle disable stream, we will also return historical streams.
    private static final String STREAM_QUERY
            = "SELECT TABLE_NAME, STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

    public static Map<String, Object> listStreams(Map<String, Object> request, String connectionUrl) {
        Map<String, Object> result = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            List<Map<String, Object>> streams = new ArrayList<>();
            String query = request.get(ApiMetadata.TABLE_NAME) == null
                    ? STREAM_QUERY + "AND SUBSTR(TABLE_NAME, 0, 4) = 'DDB.'"
                    : STREAM_QUERY + " AND TABLE_NAME = '" + "DDB." + request.get(ApiMetadata.TABLE_NAME) + "'";
            ResultSet rs = connection.createStatement().executeQuery(query);
            while (rs.next()) {
                String tableName = rs.getString(1);
                String streamName = rs.getString(2);
                long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
                Map<String, Object> stream = new HashMap<>();
                stream.put(ApiMetadata.TABLE_NAME, tableName.startsWith("DDB.") ? tableName.split("DDB.")[1] : tableName);
                stream.put(ApiMetadata.STREAM_ARN, streamName);
                stream.put(ApiMetadata.STREAM_LABEL, DDBShimCDCUtils.getStreamLabel(creationTS));
                streams.add(stream);
            }
            result.put(ApiMetadata.STREAMS, streams);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
