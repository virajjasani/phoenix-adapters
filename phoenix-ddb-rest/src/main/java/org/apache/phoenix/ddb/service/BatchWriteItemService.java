package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteItemService {

    private static final int NUM_ITEMS_LIMIT_PER_TABLE = 25;

    public static Map<String, Object> batchWriteItem(Map<String, Object> request,
            String connectionUrl) {
        Map<String, Object> unprocessedItems = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(false);
            Map<String, List<Map<String, Object>>> requestItems =
                    (Map<String, List<Map<String, Object>>>) request.get("RequestItems");
            for (Map.Entry<String, List<Map<String, Object>>> requestItemEntry
                    : requestItems.entrySet()) {
                List<Map<String, Object>> writeRequests = requestItemEntry.getValue();
                for (int i = 0;
                     i < Integer.min(writeRequests.size(), NUM_ITEMS_LIMIT_PER_TABLE); i++) {
                    Map<String, Object> wr = writeRequests.get(i);
                    if (wr.containsKey("PutRequest")) {
                        Map<String, Object> putRequest = new HashMap<>();
                        putRequest.put("Item",
                                ((Map<String, Object>) wr.get("PutRequest")).get("Item"));
                        putRequest.put("TableName", requestItemEntry.getKey());
                        PutItemService.putItemWithConn(connection, putRequest);
                    } else if (wr.containsKey("DeleteRequest")) {
                        Map<String, Object> deleteRequest = new HashMap<>();
                        deleteRequest.put("Key",
                                ((Map<String, Object>) wr.get("DeleteRequest")).get("Key"));
                        deleteRequest.put("TableName", requestItemEntry.getKey());
                        DeleteItemService.deleteItemWithConn(connection, deleteRequest);
                    } else {
                        throw new RuntimeException(
                                "WriteRequest should have either a PutRequest or a DeleteRequest.");
                    }
                }
                if (writeRequests.size() > NUM_ITEMS_LIMIT_PER_TABLE) {
                    unprocessedItems.put(requestItemEntry.getKey(),
                            writeRequests.subList(NUM_ITEMS_LIMIT_PER_TABLE, writeRequests.size()));
                }
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (unprocessedItems.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> unprocessedItemsMap = new HashMap<>();
        unprocessedItemsMap.put("UnprocessedItems", unprocessedItems);
        return unprocessedItemsMap;
    }
}
