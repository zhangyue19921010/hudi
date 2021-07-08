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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringPlanStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

@SuppressWarnings("checkstyle:LineLength")
public class SparkClusteringPlanActionExecutor<T extends HoodieRecordPayload> extends
    BaseClusteringPlanActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {

  private static final Logger LOG = LogManager.getLogger(SparkClusteringPlanActionExecutor.class);

  public SparkClusteringPlanActionExecutor(HoodieEngineContext context,
                                           HoodieWriteConfig config,
                                           HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> table,
                                           String instantTime,
                                           Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, extraMetadata);
  }

  @Override
  protected Option<HoodieClusteringPlan> createClusteringPlan() {
    LOG.info("Checking if clustering needs to be run on " + config.getBasePath());
    // 获取最后一次完成的Replace Instant => replacecommit
    Option<HoodieInstant> lastClusteringInstant = table.getActiveTimeline().getCompletedReplaceTimeline().lastInstant();

    // count 自lastClusteringInstant后有多少少个Commit-Complete Instant -> COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION
    int commitsSinceLastClustering = table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .findInstantsAfter(lastClusteringInstant.map(HoodieInstant::getTimestamp).orElse("0"), Integer.MAX_VALUE)
        .countInstants();

    // 判断是否满足 hoodie.clustering.inline.max.commits
    // 满足则返回Empty
    if (config.getInlineClusterMaxCommits() > commitsSinceLastClustering) {
      LOG.info("Not scheduling clustering as only " + commitsSinceLastClustering
          + " commits was found since last clustering " + lastClusteringInstant + ". Waiting for "
          + config.getInlineClusterMaxCommits());
      return Option.empty();
    }

    LOG.info("Generating clustering plan for table " + config.getBasePath());
    // 这里可以通过 hoodie.clustering.plan.strategy.class 自定义clustering plan 策略
    // 默认使用 org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy
    ClusteringPlanStrategy strategy = (ClusteringPlanStrategy)
        ReflectionUtils.loadClass(config.getClusteringPlanStrategyClass(), table, context, config);
    return strategy.generateClusteringPlan();
  }

}
