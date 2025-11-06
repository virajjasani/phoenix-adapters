/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.service.utils;

import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;

import java.util.List;
import java.util.Map;

import static org.apache.phoenix.ddb.utils.ApiMetadata.DELETE_LEGACY_PARAMS;
import static org.apache.phoenix.ddb.utils.ApiMetadata.GET_LEGACY_PARAMS;
import static org.apache.phoenix.ddb.utils.ApiMetadata.PUT_LEGACY_PARAMS;
import static org.apache.phoenix.ddb.utils.ApiMetadata.QUERY_LEGACY_PARAMS;
import static org.apache.phoenix.ddb.utils.ApiMetadata.SCAN_LEGACY_PARAMS;
import static org.apache.phoenix.ddb.utils.ApiMetadata.UPDATE_LEGACY_PARAMS;

/**
 * Validation for various API requests.
 */
public class ValidationUtil {

    public static void validatePutItemRequest(Map<String, Object> request) {
        validateLegacyParams(request, PUT_LEGACY_PARAMS);
    }

    public static void validateUpdateItemRequest(Map<String, Object> request) {
        validateLegacyParams(request, UPDATE_LEGACY_PARAMS);
        String updateExpression = (String) request.get(ApiMetadata.UPDATE_EXPRESSION);
        Map<String, Object> attributeUpdates =
                (Map<String, Object>) request.get(ApiMetadata.ATTRIBUTE_UPDATES);
        if (updateExpression != null && attributeUpdates != null) {
            throw new ValidationException(
                    "Cannot specify both UpdateExpression and AttributeUpdates");
        }
        String conditionExpression = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, Object> expected = (Map<String, Object>) request.get(ApiMetadata.EXPECTED);
        if (conditionExpression != null && expected != null) {
            throw new ValidationException("Cannot specify both ConditionExpression and Expected");
        }
    }

    public static void validateDeleteItemRequest(Map<String, Object> request) {
        validateLegacyParams(request, DELETE_LEGACY_PARAMS);
    }

    public static void validateGetItemRequest(Map<String, Object> request) {
        validateLegacyParams(request, GET_LEGACY_PARAMS);

        String projectionExpression = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Object attributesToGet = request.get(ApiMetadata.ATTRIBUTES_TO_GET);
        if (projectionExpression != null && attributesToGet != null) {
            throw new ValidationException(
                    "Cannot specify both ProjectionExpression and AttributesToGet");
        }
    }

    public static void validateQueryRequest(Map<String, Object> request) {
        validateLegacyParams(request, QUERY_LEGACY_PARAMS);

        String keyConditionExpression = (String) request.get(ApiMetadata.KEY_CONDITION_EXPRESSION);
        Object keyConditions = request.get(ApiMetadata.KEY_CONDITIONS);
        if (keyConditionExpression != null && keyConditions != null) {
            throw new ValidationException(
                    "Cannot specify both KeyConditionExpression and KeyConditions");
        }

        String filterExpression = (String) request.get(ApiMetadata.FILTER_EXPRESSION);
        Object queryFilter = request.get(ApiMetadata.QUERY_FILTER);
        if (filterExpression != null && queryFilter != null) {
            throw new ValidationException(
                    "Cannot specify both FilterExpression and QueryFilter");
        }

        String projectionExpression = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Object attributesToGet = request.get(ApiMetadata.ATTRIBUTES_TO_GET);
        if (projectionExpression != null && attributesToGet != null) {
            throw new ValidationException(
                    "Cannot specify both ProjectionExpression and AttributesToGet");
        }
    }

    public static void validateScanRequest(Map<String, Object> request) {
        validateLegacyParams(request, SCAN_LEGACY_PARAMS);

        String filterExpression = (String) request.get(ApiMetadata.FILTER_EXPRESSION);
        Object scanFilter = request.get(ApiMetadata.SCAN_FILTER);
        if (filterExpression != null && scanFilter != null) {
            throw new ValidationException("Cannot specify both FilterExpression and ScanFilter");
        }

        String projectionExpression = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Object attributesToGet = request.get(ApiMetadata.ATTRIBUTES_TO_GET);
        if (projectionExpression != null && attributesToGet != null) {
            throw new ValidationException(
                    "Cannot specify both ProjectionExpression and AttributesToGet");
        }
    }

    private static void validateLegacyParams(Map<String, Object> request,
                                            List<String> legacyParams) {
        for (String param : legacyParams) {
            if (request.containsKey(param)) {
                throw new ValidationException("Legacy parameter '" + param + "' is not supported.");
            }
        }
    }
}
