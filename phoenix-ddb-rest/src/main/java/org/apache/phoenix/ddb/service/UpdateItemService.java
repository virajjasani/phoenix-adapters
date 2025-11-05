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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
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
            StatementInfo statementInfo = getPreparedStatement(connection, request, pkCols.size());
            setValuesOnPreparedStatement(statementInfo, pkCols, request);

            //execute, auto commit is on
            LOGGER.debug("Upsert Query for UpdateItem: {}", statementInfo.stmt);
            Map<String, Object> returnAttrs =
                    DMLUtils.executeUpdate(statementInfo.stmt, (String) request.get(ApiMetadata.RETURN_VALUES),
                            (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                            statementInfo.conditionExpression, pkCols, false);
            return returnAttrs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static StatementInfo getPreparedStatement(Connection conn,
            Map<String, Object> request, int numPKs) throws SQLException {
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
        
        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc =
                    CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                            exprAttrVals);
            String QUERY_FORMAT = (numPKs == 1) ?
                    CONDITIONAL_UPDATE_WITH_HASH_KEY :
                    CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName));
        } else {
            String QUERY_FORMAT = (numPKs == 1) ? UPDATE_WITH_HASH_KEY : UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName));
        }

        return new StatementInfo(stmt, conditionDoc, updateDoc, condExpr);
    }

    /**
     * Set values on the prepared statement: keys, condition and update documents.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> request) throws SQLException {
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols, (Map<String, Object>) request.get(ApiMetadata.KEY));
        int paramIndex = pkCols.size() + 1;
        if (statementInfo.conditionDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.conditionDoc);
        }
        statementInfo.stmt.setObject(paramIndex, statementInfo.updateDoc);
    }

    /**
     * Helper class to return statement information including condition and update documents.
     */
    private static class StatementInfo {

        final PreparedStatement stmt;
        final BsonDocument conditionDoc;
        final BsonDocument updateDoc;
        final String conditionExpression;

        public StatementInfo(PreparedStatement stmt, BsonDocument conditionDoc,
                BsonDocument updateDoc, String conditionExpression) {
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
            this.updateDoc = updateDoc;
            this.conditionExpression = conditionExpression;
        }
    }
}
