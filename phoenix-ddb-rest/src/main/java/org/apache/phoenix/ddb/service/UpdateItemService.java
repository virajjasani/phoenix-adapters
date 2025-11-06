package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    public static Map<String, Object> updateItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateUpdateItemRequest(request);
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(new PTableKey(phoenixConnection.getTenantId(),
                    "DDB." + request.get(ApiMetadata.TABLE_NAME)));
            List<PColumn> pkCols = table.getPKColumns();

            //create statement based on PKs and conditional expression
            StatementInfo statementInfo = getPreparedStatement(connection, request, pkCols);
            setValuesOnPreparedStatement(statementInfo, pkCols, request);

            //execute, auto commit is on
            LOGGER.debug("Upsert Query for UpdateItem: {}", statementInfo.stmt);
            // Determine if any condition expression is present
            boolean hasCondExp = (request.get(ApiMetadata.CONDITION_EXPRESSION) != null) || (
                    request.get(ApiMetadata.EXPECTED) != null);

            Map<String, Object> returnAttrs = DMLUtils.executeUpdate(statementInfo.stmt,
                    (String) request.get(ApiMetadata.RETURN_VALUES),
                    (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                    hasCondExp, pkCols, false);
            return returnAttrs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static StatementInfo getPreparedStatement(Connection conn, Map<String, Object> request,
            List<PColumn> pkCols) throws SQLException {
        PreparedStatement stmt;
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);

        // Handle legacy Expected parameter conversion to ConditionExpression
        Map<String, Object> expected = (Map<String, Object>) request.get(ApiMetadata.EXPECTED);
        String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
        if (condExpr == null && expected != null) {
            // Initialize maps if they don't exist (for Expected conversion)
            if (exprAttrNames == null) {
                exprAttrNames = new HashMap<>();
            }
            if (exprAttrVals == null) {
                exprAttrVals = new HashMap<>();
            }
            // Convert Expected to ConditionExpression
            condExpr = CommonServiceUtils.convertExpectedToConditionExpression(expected,
                    conditionalOperator, exprAttrNames, exprAttrVals);
        }

        String updateExpression = (String) request.get(ApiMetadata.UPDATE_EXPRESSION);
        Map<String, Object> attributeUpdates =
                (Map<String, Object>) request.get(ApiMetadata.ATTRIBUTE_UPDATES);
        BsonDocument updateDoc;
        if (updateExpression != null) {
            updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromMap(updateExpression,
                    exprAttrNames, exprAttrVals);
        } else {
            updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromAttributeUpdates(
                    attributeUpdates);
        }
        BsonDocument conditionDoc = null;

        // Extract $SET portion from updateDoc for VALUES() clause
        BsonDocument setDoc = extractSetDocument(updateDoc, request, pkCols);

        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc = CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                    exprAttrVals);
            String queryFormat;
            if (setDoc != null) {
                // Use UPDATE when we have $SET document to provide in VALUES()
                queryFormat = (pkCols.size() == 1) ?
                        CONDITIONAL_UPDATE_WITH_HASH_KEY :
                        CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            } else {
                // Use UPDATE_ONLY when no $SET document (no initial values for new rows)
                queryFormat = (pkCols.size() == 1) ?
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY :
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY;
            }
            stmt = conn.prepareStatement(String.format(queryFormat, "DDB", tableName));
        } else {
            String QUERY_FORMAT;
            if (setDoc != null) {
                // Use UPDATE when we have $SET document to provide in VALUES()
                QUERY_FORMAT =
                        (pkCols.size() == 1) ? UPDATE_WITH_HASH_KEY : UPDATE_WITH_HASH_SORT_KEY;
            } else {
                // Use UPDATE_ONLY when no $SET document (no initial values for new rows)
                QUERY_FORMAT = (pkCols.size() == 1) ?
                        UPDATE_ONLY_WITH_HASH_KEY :
                        UPDATE_ONLY_WITH_HASH_SORT_KEY;
            }
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName));
        }
        return new StatementInfo(stmt, conditionDoc, updateDoc, setDoc);
    }

    /**
     * Extract the $SET portion from the update document and merge with primary key values
     * to use in VALUES() clause for new row creation.
     */
    private static BsonDocument extractSetDocument(BsonDocument updateDoc,
            Map<String, Object> request, List<PColumn> pkCols) {
        if (updateDoc != null && updateDoc.containsKey("$SET")) {
            BsonDocument setDoc = updateDoc.getDocument("$SET").clone();
            Map<String, Object> keyMap = (Map<String, Object>) request.get(ApiMetadata.KEY);
            if (keyMap != null) {
                for (PColumn pkCol : pkCols) {
                    String keyName = pkCol.getName().getString();
                    Object keyValue = keyMap.get(keyName);
                    if (keyValue != null && keyValue instanceof Map) {
                        BsonValue value = MapToBsonDocument.getValueFromMapVal(
                                (Map<String, Object>) keyValue);
                        setDoc.put(keyName, value);
                    }
                }
            }
            return setDoc;
        }
        return null;
    }

    /**
     * Set values on the prepared statement: keys, $SET document (if present),
     * condition and update documents.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> request) throws SQLException {
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols,
                (Map<String, Object>) request.get(ApiMetadata.KEY));
        int paramIndex = pkCols.size() + 1;

        // Set the $SET document for VALUES() clause (only for UPDATE flavors, not UPDATE_ONLY)
        if (statementInfo.setDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.setDoc);
        }

        if (statementInfo.conditionDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.conditionDoc);
        }
        statementInfo.stmt.setObject(paramIndex, statementInfo.updateDoc);
    }

    /**
     * Helper class to return statement information including condition, update, and set documents.
     */
    private static class StatementInfo {

        final PreparedStatement stmt;
        final BsonDocument conditionDoc;
        final BsonDocument updateDoc;
        final BsonDocument setDoc;

        public StatementInfo(PreparedStatement stmt, BsonDocument conditionDoc,
                BsonDocument updateDoc, BsonDocument setDoc) {
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
            this.updateDoc = updateDoc;
            this.setDoc = setDoc;
        }
    }
}
