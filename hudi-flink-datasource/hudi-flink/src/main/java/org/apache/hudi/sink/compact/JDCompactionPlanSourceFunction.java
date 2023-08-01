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

package org.apache.hudi.sink.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.compact.strategy.CompactionPlanStrategies;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.CompactionUtil;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Flink hudi compaction source function.
 *
 * <P>This function read the compaction plan as {@link CompactionOperation}s then assign the compaction task
 * event {@link CompactionPlanEvent} to downstream operators.
 *
 * <p>The compaction instant time is specified explicitly with strategies:
 *
 * <ul>
 *   <li>If the timeline has no inflight instants,
 *   use {@link org.apache.hudi.common.table.timeline.HoodieActiveTimeline#createNewInstantTime()}
 *   as the instant time;</li>
 *   <li>If the timeline has inflight instants,
 *   use the median instant time between [last complete instant time, earliest inflight instant time]
 *   as the instant time.</li>
 * </ul>
 */
public class JDCompactionPlanSourceFunction extends CompactionPlanSourceFunction {

  protected static final Logger LOG = LoggerFactory.getLogger(JDCompactionPlanSourceFunction.class);

  /**
   * compaction plan instant -> compaction plan
   */
  private List<Pair<String, HoodieCompactionPlan>> compactionPlans;
  private final HoodieFlinkTable<?> table;

  private final HoodieFlinkWriteClient<?> writeClient;

  private final FlinkCompactionConfig cfg;

  public JDCompactionPlanSourceFunction(HoodieFlinkTable<?> table, HoodieFlinkWriteClient<?> writeClient, FlinkCompactionConfig cfg) {
    super(new ArrayList<>());
    this.table = table;
    this.writeClient = writeClient;
    this.cfg = cfg;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (noCompactionPlans()) {
      // generate compaction plans
      this.compactionPlans = getCompactionPlans();
    }
  }

  @Override
  public void run(SourceContext sourceContext) throws Exception {
    for (Pair<String, HoodieCompactionPlan> pair : compactionPlans) {
      HoodieCompactionPlan compactionPlan = pair.getRight();
      List<CompactionOperation> operations = compactionPlan.getOperations().stream()
          .map(CompactionOperation::convertFromAvroRecordInstance).collect(Collectors.toList());
      LOG.info("CompactionPlanFunction compacting " + operations + " files");
      for (CompactionOperation operation : operations) {
        sourceContext.collect(new CompactionPlanEvent(pair.getLeft(), operation));
      }
    }
  }

  @Override
  public void close() throws Exception {
    // no operation
  }

  @Override
  public void cancel() {
    // no operation
  }

  private boolean noCompactionPlans() {
    return compactionPlans == null || compactionPlans.isEmpty();
  }

  private List<Pair<String, HoodieCompactionPlan>> getCompactionPlans() {
    table.getMetaClient().reloadActiveTimeline();

    // checks the compaction plan and do compaction.
    if (cfg.schedule) {
      Option<String> compactionInstantTimeOption = CompactionUtil.getCompactionInstantTime(table.getMetaClient());
      if (compactionInstantTimeOption.isPresent()) {
        boolean scheduled = writeClient.scheduleCompactionAtInstant(compactionInstantTimeOption.get(), Option.empty());
        if (!scheduled) {
          // do nothing.
          LOG.info("No compaction plan for this job ");
          return new ArrayList<>();
        }
        table.getMetaClient().reloadActiveTimeline();
      }
    }

    // fetch the instant based on the configured execution sequence
    HoodieTimeline pendingCompactionTimeline = table.getActiveTimeline().filterPendingCompactionTimeline();
    List<HoodieInstant> requested = CompactionPlanStrategies.getStrategy(cfg).select(pendingCompactionTimeline);
    if (requested.isEmpty()) {
      // do nothing.
      LOG.info("No compaction plan scheduled, turns on the compaction plan schedule with --schedule option");
      return new ArrayList<>();
    }

    List<String> compactionInstantTimes = requested.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    compactionInstantTimes.forEach(timestamp -> {
      HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(timestamp);
      if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
        LOG.info("Rollback inflight compaction instant: [" + timestamp + "]");
        table.rollbackInflightCompaction(inflightInstant);
        table.getMetaClient().reloadActiveTimeline();
      }
    });

    // generate timestamp and compaction plan pair
    // should support configurable commit metadata
    List<Pair<String, HoodieCompactionPlan>> compactionPlans = compactionInstantTimes.stream()
        .map(timestamp -> {
          try {
            return Pair.of(timestamp, CompactionUtils.getCompactionPlan(table.getMetaClient(), timestamp));
          } catch (Exception e) {
            throw new HoodieException("Get compaction plan at instant " + timestamp + " error", e);
          }
        })
        // reject empty compaction plan
        .filter(pair -> validCompactionPlan(pair.getRight()))
        .collect(Collectors.toList());

    if (compactionPlans.isEmpty()) {
      // No compaction plan, do nothing and return.
      LOG.info("No compaction plan for instant " + String.join(",", compactionInstantTimes));
      return new ArrayList<>();
    }

    List<HoodieInstant> instants = compactionInstantTimes.stream().map(HoodieTimeline::getCompactionRequestedInstant).collect(Collectors.toList());
    for (HoodieInstant instant : instants) {
      if (!pendingCompactionTimeline.containsInstant(instant)) {
        // this means that the compaction plan was written to auxiliary path(.tmp)
        // but not the meta path(.hoodie), this usually happens when the job crush
        // exceptionally.
        // clean the compaction plan in auxiliary path and cancels the compaction.
        LOG.warn("The compaction plan was fetched through the auxiliary path(.tmp) but not the meta path(.hoodie).\n"
            + "Clean the compaction plan in auxiliary path and cancels the compaction");
        CompactionUtil.cleanInstant(table.getMetaClient(), instant);
        return new ArrayList<>();
      }
    }

    LOG.info("Start to compaction for instant " + compactionInstantTimes);

    // Mark instant as compaction inflight
    for (HoodieInstant instant : instants) {
      table.getActiveTimeline().transitionCompactionRequestedToInflight(instant);
    }
    table.getMetaClient().reloadActiveTimeline();

    return compactionPlans;
  }

  private static boolean validCompactionPlan(HoodieCompactionPlan plan) {
    return plan != null && plan.getOperations() != null && plan.getOperations().size() > 0;
  }

}
