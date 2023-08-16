/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.savepoint;

public enum SavepointTriggerStrategy {
    // trigger savepoint when reach N delta commits
    NUM_COMMITS,
    // trigger savepoint when reach N delta commits since last savepoint request
    NUM_COMMITS_AFTER_LAST_REQUEST,
    // trigger savepoint when time elapsed > N seconds since last compaction
    TIME_ELAPSED,
    // trigger savepoint when both NUM_COMMITS and TIME_ELAPSED are satisfied
    NUM_AND_TIME,
    // trigger savepoint when NUM_COMMITS or TIME_ELAPSED is satisfied
    NUM_OR_TIME
}
