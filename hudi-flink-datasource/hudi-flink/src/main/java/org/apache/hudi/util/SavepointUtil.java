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

package org.apache.hudi.util;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.sink.event.WatermarkEvent;
import org.apache.hudi.table.action.savepoint.SavepointHelpers;
import org.apache.hudi.table.action.savepoint.SavepointTriggerStrategy;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Utilities for flink hudi savepoint.
 * Same as ScheduleCompactionActionExecutor#needCompact()
 */
public class SavepointUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SavepointUtil.class);
  public static Pair<Boolean, Long> needSavepoint(SavepointTriggerStrategy strategy, ConcurrentHashMap<Integer, Option<WatermarkEvent>> watermarkMap, Date nextSavepointDate) {
    Option<Long> minWatermark = Option.empty();
    switch (strategy) {
      case NUM_COMMITS:
      case NUM_COMMITS_AFTER_LAST_REQUEST:
      case TIME_ELAPSED:
      case NUM_OR_TIME:
      case NUM_AND_TIME: {
        for (Option<WatermarkEvent> event : watermarkMap.values()) {
          if (event == null || !event.isPresent()) {
            LOG.info("There is empty watermarks, skip current savepoint");
            return Pair.of(false, null);
          }
          Long watermarkTime = event.get().getWatermarkTime();

          minWatermark = minWatermark.isPresent() ? Option.of(Math.min(minWatermark.get(), watermarkTime)) : Option.of(watermarkTime);
        }

        if (minWatermark.isPresent() && new Date(minWatermark.get()).compareTo(nextSavepointDate) > 0) {
          return Pair.of(true, minWatermark.get());
        }
        break;
      }
      default:
        throw new HoodieCompactionException("Unsupported compaction trigger strategy: " + strategy);
    }

    if (minWatermark.isPresent()) {
      long unSatisfied = watermarkMap.size() - watermarkMap.values().stream().filter(event -> {
        return event != null && event.isPresent();
      }).filter(watermarkEvent -> {
        return new Date(watermarkEvent.get().getWatermarkTime()).compareTo(nextSavepointDate) > 0;
      }).count();
      LOG.info(String.format("Still %s / %s watermarks are not satisfied %s, skip to doing savepoint",
          unSatisfied, watermarkMap.size(), nextSavepointDate));
    } else {
      LOG.info("There is no watermark from tasks, skip doing savepoint");
    }

    return Pair.of(false, null);
  }

  public static boolean doSavepoint(ConcurrentHashMap<Integer, Option<WatermarkEvent>> watermarkMap, HoodieFlinkWriteClient writeClient,
                                    String instant, Date nextSavepointDate) {
    HoodieTimeline savePointTimeline = writeClient.getHoodieTable().getActiveTimeline().getSavePointTimeline();
    HoodieTimeline completedSavepoint = savePointTimeline.filterCompletedInstants();
    if (completedSavepoint.containsInstant(instant)) {
      Log.info("Current instant + " + instant + "is already be savepointed.");
      return true;
    }
    Pair<Boolean, Long> needSavepoint = SavepointUtil.needSavepoint(writeClient.getConfig().getSavepointStrategy(), watermarkMap, nextSavepointDate);
    if (needSavepoint.getLeft()) {
      // clean last failed savepoint action
      HoodieTimeline pendingSavepoint = savePointTimeline.filterInflightsAndRequested();
      if (pendingSavepoint.containsInstant(instant)) {
        LOG.info("Rollback last failed savepoint action first : " + instant);
        SavepointHelpers.deleteSavepoint(writeClient.getHoodieTable(), instant);
      }

      LOG.info("Starting to do savepoint based on event time " + needSavepoint.getRight());
      writeClient.savepoint(instant, "", "", String.valueOf(needSavepoint.getRight()));
      LOG.info("Savepoint finished based on event time " + needSavepoint.getRight() + " at " + instant);
      return true;
    }
    return false;
  }

  public static boolean isSavepointInProgress(ConcurrentHashMap<Integer, Option<WatermarkEvent>> watermarkMap, String instant, Date nextSavepointDate) {

    Optional<Long> maxWatermark = watermarkMap.values().stream()
        .filter(event -> event != null && event.isPresent())
        .peek(watermark -> LOG.info("Got watermark " + new Date(watermark.get().getWatermarkTime()) + " from task ID " + watermark.get().getTaskID()))
        .map(event -> event.get().getWatermarkTime())
        .max(Long::compareTo);

    if (maxWatermark.isPresent() && new Date(maxWatermark.get()).compareTo(nextSavepointDate) > 0) {
      LOG.info(String.format("Auto Savepoint is in progress. Received max watermark from is %s, next savepoint date is %s",
          new Date(maxWatermark.get()), nextSavepointDate));
      return true;
    }

    LOG.info(String.format("Auto Savepoint is not in progress. Received max watermark from is %s, next savepoint date is %s",
        maxWatermark.isPresent() ? new Date(maxWatermark.get()) : "", nextSavepointDate));
    return false;
  }
}
