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

import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SavepointHelpers {

  private static final Logger LOG = LogManager.getLogger(SavepointHelpers.class);

  public static void deleteSavepoint(HoodieTable table, String savepointTime) {
    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresentInCompleted = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresentInCompleted) {
      HoodieTimeline pending = table.getMetaClient().getActiveTimeline().getSavePointTimeline().filterInflightsAndRequested();
      if (pending.containsInstant(savepointTime)) {
        table.getActiveTimeline().deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
        LOG.info("Inflight Savepoint " + savepointTime + " deleted");
        return;
      }
      LOG.warn("No savepoint present " + savepointTime);
      return;
    }

    table.getActiveTimeline().revertToInflight(savePoint);
    table.getActiveTimeline().deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
    LOG.info("Savepoint " + savepointTime + " deleted");
  }

  public static void validateSavepointRestore(HoodieTable table, String savepointTime) {
    // Make sure the restore was successful
    table.getMetaClient().reloadActiveTimeline();
    Option<HoodieInstant> lastInstant = table.getActiveTimeline()
        .getWriteTimeline()
        .filterCompletedAndCompactionInstants()
        .lastInstant();
    ValidationUtils.checkArgument(lastInstant.isPresent());
    ValidationUtils.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
        savepointTime + " is not the last commit after restoring to savepoint, last commit was "
            + lastInstant.get().getTimestamp());
  }

  public static void validateSavepointPresence(HoodieTable table, String savepointTime) {
    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for instantTime " + savepointTime);
    }
  }

  public static String getSavepointEndCommit(HoodieTableMetaClient metaClient, String savepointReadDate) {
    String savepointInstantTime = null;
    if (savepointReadDate != null) {
      try {
        long pickupEventTimestamp = Long.MAX_VALUE;
        long savepointReadTimestamp = HoodieInstantTimeGenerator.parseDateFromInstantTime(savepointReadDate).getTime();
        HoodieTimeline timeline = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
        for (HoodieInstant instant : timeline.getInstants()) {
          String instantTimestamp = instant.getTimestamp();
          HoodieSavepointMetadata metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(
              timeline.getInstantDetails(instant).get());
          String recordMinEventTime = metadata.getRecordMinEventTime();
          long recordMinEventTimestamp = Long.parseLong(recordMinEventTime);
          if (recordMinEventTimestamp > savepointReadTimestamp && recordMinEventTimestamp < pickupEventTimestamp) {
            savepointInstantTime = instantTimestamp;
            pickupEventTimestamp = recordMinEventTimestamp;
          }
        }
      } catch (Exception e) {
        LOG.warn("Error when reading savepoint from instant timeline.", e);
      }
    } else {
      // get the latest savepoint
      savepointInstantTime = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants().lastInstant().get().getTimestamp();
    }
    LOG.info("Savepoint view read up to " + savepointInstantTime);
    return savepointInstantTime;
  }

  public static List<FileStatus> convertFileSliceIntoFileStatusUnchecked(List<FileSlice> fileSlices) {
    return fileSlices.parallelStream().flatMap(fileSlice -> {
      Option<HoodieBaseFile> baseFile = fileSlice.getBaseFile();
      List<FileStatus> fileStatuses = fileSlice.getLogFiles().map(HoodieLogFile::getFileStatus).collect(Collectors.toList());
      if (baseFile.isPresent()) {
        fileStatuses.add(baseFile.get().getFileStatus());
      }
      return fileStatuses.stream();
    }).collect(Collectors.toList());
  }

  public static List<String> getPartitionsFromSavepoint(HoodieTableMetaClient metaClient, String instantTime) {
    HoodieTimeline timeline = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
    try {
      HoodieSavepointMetadata metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(
          timeline.getInstantDetails(new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, instantTime)).get());
      return metadata.getPartitionMetadata().values().stream()
          .map(HoodieSavepointPartitionMetadata::getPartitionPath).collect(Collectors.toList());
    } catch (IOException e) {
      LOG.warn("Error when reading savepoint from instant timeline.", e);
    }
    return new ArrayList<>();
  }

  public static Long getBoundaryFromInstant(String instant, HoodieTableMetaClient metaClient, boolean filterBySavepointEventTime) {
    if (!filterBySavepointEventTime) {
      return null;
    }
    try {
      HoodieTimeline spTimeline = metaClient.getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
      HoodieSavepointMetadata metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(
          spTimeline.getInstantDetails(new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, instant)).get());
      return Long.parseLong(metadata.getSavepointDateBoundary());
    } catch (Exception e) {
      throw new HoodieException("", e);
    }
  }
}
