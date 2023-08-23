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
import org.apache.hudi.table.action.savepoint.SavepointHelpers;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Utilities for flink hudi savepoint.
 * Same as ScheduleCompactionActionExecutor#needCompact()
 */
public class SavepointUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SavepointUtil.class);
  public static Boolean needSavepoint(Option<Long> eventTimeMin, Date nextSavepointDate) {
    if (eventTimeMin.isPresent() && new Date(eventTimeMin.get()).compareTo(nextSavepointDate) > 0) {
      return true;
    } else {
      LOG.info(String.format("Event time min %s are not satisfied %s, skip to doing savepoint",
          eventTimeMin.isPresent() ? new Date(eventTimeMin.get()) : null, nextSavepointDate));
      return false;
    }
  }

  public static boolean doSavepoint(Option<Long> eventTimeMin, HoodieFlinkWriteClient writeClient,
                                    String instant, Date savepointDateBoundary) {
    if (needSavepoint(eventTimeMin, savepointDateBoundary)) {
      return doSavepointDirectly(eventTimeMin, writeClient, instant, savepointDateBoundary.getTime());
    }
    return false;
  }

  public static boolean doSavepointDirectly(Option<Long> recordMinEventTime, HoodieFlinkWriteClient writeClient, String instant, long savepointDateBoundary) {
    HoodieTimeline savePointTimeline = writeClient.getHoodieTable().getActiveTimeline().getSavePointTimeline();
    HoodieTimeline completedSavepoint = savePointTimeline.filterCompletedInstants();
    if (completedSavepoint.containsInstant(instant)) {
      Log.info("Current instant + " + instant + "is already be savepointed.");
      return true;
    }

    // clean last failed savepoint action
    HoodieTimeline pendingSavepoint = savePointTimeline.filterInflightsAndRequested();
    if (pendingSavepoint.containsInstant(instant)) {
      LOG.info("Rollback last failed savepoint action first : " + instant);
      SavepointHelpers.deleteSavepoint(writeClient.getHoodieTable(), instant);
    }

    LOG.info("Starting to do savepoint based on " + recordMinEventTime);
    writeClient.savepoint(instant, "", "", String.valueOf(recordMinEventTime.get()), String.valueOf(savepointDateBoundary));
    LOG.info("Savepoint finished based on " + recordMinEventTime.get() + " at " + instant);
    return true;
  }

  public static boolean isSavepointInProgress(Option<Long> eventTimeMax, Date nextSavepointDate) {
    if (eventTimeMax.isPresent() && new Date(eventTimeMax.get()).compareTo(nextSavepointDate) > 0) {
      LOG.info(String.format("Auto Savepoint is in progress. Received max event time is %s, next savepoint date is %s",
          new Date(eventTimeMax.get()), nextSavepointDate));
      return true;
    }
    LOG.info(String.format("Auto Savepoint is not in progress. Received max event time is %s, next savepoint date is %s",
        eventTimeMax.isPresent() ? new Date(eventTimeMax.get()) : "", nextSavepointDate));
    return false;
  }
}
