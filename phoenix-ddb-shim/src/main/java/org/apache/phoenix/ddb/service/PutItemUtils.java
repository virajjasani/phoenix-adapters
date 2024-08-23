package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.bson.BsonDocument;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PutItemUtils {

    private static final String UPSERT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?,?)";
    private static final String UPSERT_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?,?)";

    public static PutItemResult putItem(PutItemRequest request, String connectionUrl) {
        // get tableName, item and convert item to bson
        String tableName = request.getTableName();
        Map<String, AttributeValue> item = request.getItem();
        BsonDocument bsonDoc = DdbAttributesToBsonDocument.getBsonDocument(item);

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(
                    new PTableKey(phoenixConnection.getTenantId(), tableName));
            List<PColumn> pkCols = table.getPKColumns();
            PreparedStatement stmt = connection.prepareStatement(String.format(UPSERT_WITH_HASH_KEY, tableName));
            if (pkCols.size() == 2) {
                stmt = connection.prepareStatement(String.format(UPSERT_WITH_HASH_SORT_KEY, tableName));
            }

            // extract values for keys from the item and set them on the PreparedStatement
            for (int i=0; i<pkCols.size(); i++) {
                PColumn pkCol = pkCols.get(i);
                String colName = pkCol.getName().toString().toLowerCase();
                PDataType type = pkCol.getDataType();
                if (type.equals(PDouble.INSTANCE)) {
                    Double value = Double.parseDouble(item.get(colName).getN());
                    stmt.setDouble(i+1, value);
                } else if (type.equals(PVarchar.INSTANCE)) {
                    String value = item.get(colName).getS();
                    stmt.setString(i+1, value);
                } else if (type.equals(PVarbinaryEncoded.INSTANCE)) {
                    byte[] b = item.get(colName).getB().array();
                    stmt.setBytes(i+1, b);
                } else {
                    throw new IllegalArgumentException("Primary Key column type "
                            + type + " is not " + "correct type");
                }
            }

            // set bson document of entire item
            stmt.setObject(pkCols.size()+1, bsonDoc);

            //execute, auto commit is on
            stmt.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new PutItemResult();
    }
}
