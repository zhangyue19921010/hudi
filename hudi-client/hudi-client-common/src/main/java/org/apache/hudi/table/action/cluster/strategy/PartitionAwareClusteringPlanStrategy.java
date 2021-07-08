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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Scheduling strategy with restriction that clustering groups can only contain files from same partition.
 */
public abstract class PartitionAwareClusteringPlanStrategy<T extends HoodieRecordPayload,I,K,O> extends ClusteringPlanStrategy<T,I,K,O> {
  private static final Logger LOG = LogManager.getLogger(PartitionAwareClusteringPlanStrategy.class);

  public PartitionAwareClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  /**
   * Create Clustering group based on files eligible for clustering in the partition.
   */
  protected abstract Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath,
                                                                                     List<FileSlice> fileSlices);

  /**
   * Return list of partition paths to be considered for clustering.
   */
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    return partitionPaths;
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan() {
    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for " + metaClient.getBasePath());
    HoodieWriteConfig config = getWriteConfig();

    // 获取当前hudi表的所有partition Path ; Fetch list of all partition paths
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(getEngineContext(), config.getMetadataConfig(), metaClient.getBasePath());

    // filter the partition paths if needed to reduce list status
    // 从partition全集里根据特定策略筛选指定要clustering的partition
    partitionPaths = filterPartitionPaths(partitionPaths);

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    List<HoodieClusteringGroup> clusteringGroups = getEngineContext().flatMap(partitionPaths,
        partitionPath -> {
          // 获取当前partition下 需要且能够进行clustering的所有fileSlice，需要满足以下条件:
          // 1. 滤除 pending clustering
          // 2. 滤除 pending compaction
          // 3. 滤除 replaced filegroup
          // 4.
          // FileIds in pending clustering/compaction are not eligible(资格) for clustering.
          List<FileSlice> fileSlicesEligible = getFileSlicesEligibleForClustering(partitionPath).collect(Collectors.toList());

          //
          return buildClusteringGroupsForPartition(partitionPath, fileSlicesEligible).limit(getWriteConfig().getClusteringMaxNumGroups());
        },
        partitionPaths.size())
        .stream().limit(getWriteConfig().getClusteringMaxNumGroups()).collect(Collectors.toList());

    if (clusteringGroups.isEmpty()) {
      LOG.info("No data available to cluster");
      return Option.empty();
    }

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(getWriteConfig().getClusteringExecutionStrategyClass())
        .setStrategyParams(getStrategyParams())
        .build();

    return Option.of(HoodieClusteringPlan.newBuilder()
        .setStrategy(strategy)
        .setInputGroups(clusteringGroups)
        .setExtraMetadata(getExtraMetadata())
        .setVersion(getPlanVersion())
        .build());
  }
}
