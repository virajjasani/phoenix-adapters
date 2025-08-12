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

package org.apache.phoenix.ddb.rest.metrics;

import org.apache.hadoop.hbase.metrics.BaseSource;
import org.apache.hadoop.hbase.metrics.JvmPauseMonitorSource;

/**
 * Interface of the Metrics Source that will export data to Hadoop's Metrics2 system.
 */
public interface MetricsRESTSource extends BaseSource, JvmPauseMonitorSource {

    String METRICS_NAME = "PHOENIX-REST";
    String CONTEXT = "phoenix-rest";
    String JMX_CONTEXT = "PHOENIX-REST";
    String METRICS_DESCRIPTION = "Metrics for the Phoenix DynamoDB REST server";
    String REQUEST_KEY = "requests";

    void incrementRequests(int inc);

    void recordSuccessTime(ApiOperation operation, long time);

    void recordFailureTime(ApiOperation operation, long time);
}
