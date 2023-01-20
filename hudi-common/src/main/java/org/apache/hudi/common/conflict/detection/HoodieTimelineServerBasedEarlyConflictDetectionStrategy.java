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

package org.apache.hudi.common.conflict.detection;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import java.util.Set;

public abstract class HoodieTimelineServerBasedEarlyConflictDetectionStrategy implements HoodieEarlyConflictDetectionStrategy {

  protected final String basePath;
  protected final String markerDir;
  protected final String markerName;
  protected final boolean checkCommitConflict;

  public HoodieTimelineServerBasedEarlyConflictDetectionStrategy(String basePath, String markerDir, String markerName, Boolean checkCommitConflict) {
    this.basePath = basePath;
    this.markerDir = markerDir;
    this.markerName = markerName;
    this.checkCommitConflict = checkCommitConflict;
  }

  public void fresh(Long batchInterval, Long period, String markerDir, String basePath,
                    Long maxAllowableHeartbeatIntervalInMs, FileSystem fileSystem, Object markerHandler, Set<HoodieInstant> oldInstants) {}
}