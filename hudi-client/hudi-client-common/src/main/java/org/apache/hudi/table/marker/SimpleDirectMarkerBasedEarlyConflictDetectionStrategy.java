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

package org.apache.hudi.table.marker;

import org.apache.hudi.common.conflict.detection.HoodieDirectMarkerBasedEarlyConflictDetectionStrategy;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieEarlyConflictDetectionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file conflict.
 */
public class SimpleDirectMarkerBasedEarlyConflictDetectionStrategy extends HoodieDirectMarkerBasedEarlyConflictDetectionStrategy {

  private static final Logger LOG = LogManager.getLogger(SimpleDirectMarkerBasedEarlyConflictDetectionStrategy.class);

  public SimpleDirectMarkerBasedEarlyConflictDetectionStrategy(String basePath, HoodieWrapperFileSystem fs, String partitionPath, String fileId, String instantTime,
                                                               HoodieActiveTimeline activeTimeline, HoodieWriteConfig config) {
    super(basePath, fs, partitionPath, fileId, instantTime, activeTimeline, config);
  }

  @Override
  public boolean hasMarkerConflict() {
    try {
      return checkMarkerConflict(basePath, partitionPath, fileId, fs, instantTime) || checkCommitConflict(fileId, basePath);
    } catch (IOException e) {
      LOG.warn("Exception occurs during create marker file in eager conflict detection mode.");
      throw new HoodieIOException("Exception occurs during create marker file in eager conflict detection mode.", e);
    }
  }

  @Override
  public void resolveMarkerConflict(String basePath, String partitionPath, String dataFileName) {
    throw new HoodieEarlyConflictDetectionException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }

  @Override
  public void detectAndResolveConflictIfNecessary() {

    if (hasMarkerConflict()) {
      resolveMarkerConflict(basePath, partitionPath, fileId);
    }
  }
}
