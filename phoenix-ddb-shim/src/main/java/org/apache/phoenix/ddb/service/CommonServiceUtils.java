/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

public class CommonServiceUtils {

    public static ScalarAttributeType getScalarAttributeFromPDataType(PDataType<?> pDataType) {
        if (pDataType == PVarchar.INSTANCE) {
            return ScalarAttributeType.S;
        } else if (pDataType == PDouble.INSTANCE || pDataType == PDecimal.INSTANCE) {
            return ScalarAttributeType.N;
        } else if (pDataType == PVarbinaryEncoded.INSTANCE) {
            return ScalarAttributeType.B;
        } else {
            throw new IllegalStateException("Invalid data type: " + pDataType.toString());
        }
    }

    public static String getKeyNameFromBsonValueFunc(String keyName) {
        if (keyName.contains("BSON_VALUE")) {
            keyName = keyName.split("BSON_VALUE")[1].split(",")[1];
            if (keyName.charAt(0) == '\'' && keyName.charAt(keyName.length() - 1) == '\'') {
                StringBuilder sb = new StringBuilder(keyName);
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(0);
                keyName = sb.toString();
            }
        }
        return keyName;
    }

    public static void updateTableDescriptorForIndexes(PTable table,
        TableDescription tableDescription) {
        if (table.getIndexes() != null && !table.getIndexes().isEmpty()) {
            List<GlobalSecondaryIndexDescription> indexList = new ArrayList<>();
            for (PTable index : table.getIndexes()) {
                GlobalSecondaryIndexDescription globalSecondaryIndexDescription =
                    new GlobalSecondaryIndexDescription();
                globalSecondaryIndexDescription.setIndexName(index.getTableName().getString());

                List<PColumn> respPkColumns = index.getPKColumns();
                List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
                String hashKeyName = respPkColumns.get(0).getName().getString();
                hashKeyName = getKeyNameFromBsonValueFunc(hashKeyName);
                KeySchemaElement hashKeyElement = new KeySchemaElement(hashKeyName, KeyType.HASH);
                respKeySchemaElements.add(hashKeyElement);
                tableDescription.getAttributeDefinitions().add(new AttributeDefinition(hashKeyName,
                    getScalarAttributeFromPDataType(
                        respPkColumns.get(0).getDataType())));
                if (respPkColumns.size() - table.getPKColumns().size() > 1) {
                    String sortKeyName = respPkColumns.get(1).getName().getString();
                    sortKeyName = getKeyNameFromBsonValueFunc(sortKeyName);
                    KeySchemaElement sortKeyElement =
                        new KeySchemaElement(sortKeyName, KeyType.RANGE);
                    respKeySchemaElements.add(sortKeyElement);
                    tableDescription.getAttributeDefinitions().add(
                        new AttributeDefinition(sortKeyName,
                            getScalarAttributeFromPDataType(
                                respPkColumns.get(1).getDataType())));
                }
                globalSecondaryIndexDescription.setKeySchema(respKeySchemaElements);
                globalSecondaryIndexDescription.setIndexStatus(IndexStatus.ACTIVE);
                indexList.add(globalSecondaryIndexDescription);
            }
            tableDescription.setGlobalSecondaryIndexes(indexList);
        }
    }

    public static TableDescription getTableDescription(String tableName, String connectionUrl) {
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), tableName));
            TableDescription tableDescription = new TableDescription();
            tableDescription.setTableName(table.getTableName().getString());
            tableDescription.setTableStatus(TableStatus.ACTIVE);

            List<PColumn> respPkColumns = table.getPKColumns();
            List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
            KeySchemaElement hashKeyElement =
                new KeySchemaElement(respPkColumns.get(0).getName().getString(), KeyType.HASH);
            respKeySchemaElements.add(hashKeyElement);
            if (respPkColumns.size() == 2) {
                KeySchemaElement sortKeyElement =
                    new KeySchemaElement(respPkColumns.get(1).getName().getString(), KeyType.RANGE);
                respKeySchemaElements.add(sortKeyElement);
            }
            tableDescription.setKeySchema(respKeySchemaElements);

            List<AttributeDefinition> respAttributeDefs = new ArrayList<>();
            AttributeDefinition hashAttribute =
                new AttributeDefinition(respPkColumns.get(0).getName().getString(),
                    getScalarAttributeFromPDataType(
                        respPkColumns.get(0).getDataType()));
            respAttributeDefs.add(hashAttribute);
            if (respPkColumns.size() == 2) {
                AttributeDefinition sortAttribute =
                    new AttributeDefinition(respPkColumns.get(1).getName().getString(),
                        getScalarAttributeFromPDataType(
                            respPkColumns.get(1).getDataType()));
                respAttributeDefs.add(sortAttribute);
            }
            tableDescription.setAttributeDefinitions(respAttributeDefs);

            updateTableDescriptorForIndexes(table, tableDescription);
            tableDescription.setCreationDateTime(new Date(table.getTimeStamp()));
            return tableDescription;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
