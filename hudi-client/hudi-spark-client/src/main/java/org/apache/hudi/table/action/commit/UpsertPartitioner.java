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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition).
 */
public class UpsertPartitioner<T extends HoodieRecordPayload<T>> extends Partitioner {

  private static final Logger LOG = LogManager.getLogger(UpsertPartitioner.class);

  /**
   * List of all small files to be corrected.
   */
  protected List<SmallFile> smallFiles = new ArrayList<>();
  /**
   * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
   */
  private int totalBuckets = 0;
  /**
   * Stat for the current workload. Helps in determining inserts, upserts etc.
   */
  private WorkloadProfile profile;
  /**
   * Helps decide which bucket an incoming update should go to.
   */
  private HashMap<String, Integer> updateLocationToBucket;
  /**
   * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
   */
  private HashMap<String, List<InsertBucketCumulativeWeightPair>> partitionPathToInsertBucketInfos;
  /**
   * Remembers what type each bucket is for later.
   */
  private HashMap<Integer, BucketInfo> bucketInfoMap;

  protected final HoodieTable table;

  protected final HoodieWriteConfig config;

  public UpsertPartitioner(WorkloadProfile profile, HoodieEngineContext context, HoodieTable table,
      HoodieWriteConfig config) {
    updateLocationToBucket = new HashMap<>();
    partitionPathToInsertBucketInfos = new HashMap<>();
    bucketInfoMap = new HashMap<>();
    this.profile = profile;
    this.table = table;
    this.config = config;
    assignUpdates(profile);
    assignInserts(profile, context);

    LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
        + "Partition to insert buckets => " + partitionPathToInsertBucketInfos + ", \n"
        + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
  }

  private void assignUpdates(WorkloadProfile profile) {
    // each update location gets a partition
    // PartitionPathStatMap [partitionPath, partitionWorkloadStat] 用于统计每个partition中有多少insets以及有哪些file有多少update
    //
    Set<Entry<String, WorkloadStat>> partitionStatEntries = profile.getPartitionPathStatMap().entrySet();
    // 遍历所有partition
    for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
      // 遍历所有update
      // updateLocEntry: [fileId, numbers]
      for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
          partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
        addUpdateBucket(partitionStat.getKey(), updateLocEntry.getKey());
      }
    }
  }

  private int addUpdateBucket(String partitionPath, String fileIdHint) {
    int bucket = totalBuckets; // bucket编号
    updateLocationToBucket.put(fileIdHint, bucket);
    BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, fileIdHint, partitionPath);
    bucketInfoMap.put(totalBuckets, bucketInfo);
    totalBuckets++;
    return bucket;
  }

  /**
   * Get the in pending clustering fileId for each partition path.
   * @return partition path to pending clustering file groups id
   */
  private Map<String, Set<String>> getPartitionPathToPendingClusteringFileGroupsId() {
    Map<String, Set<String>>  partitionPathToInPendingClusteringFileId =
        table.getFileSystemView().getFileGroupsInPendingClustering()
            .map(fileGroupIdAndInstantPair ->
                Pair.of(fileGroupIdAndInstantPair.getKey().getPartitionPath(), fileGroupIdAndInstantPair.getKey().getFileId()))
            .collect(Collectors.groupingBy(Pair::getKey, Collectors.mapping(Pair::getValue, Collectors.toSet())));
    return partitionPathToInPendingClusteringFileId;
  }

  /**
   * Exclude small file handling for clustering since update path is not supported.
   * @param pendingClusteringFileGroupsId  pending clustering file groups id of partition
   * @param smallFiles small files of partition
   * @return smallFiles not in clustering
   */
  private List<SmallFile> filterSmallFilesInClustering(final Set<String> pendingClusteringFileGroupsId, final List<SmallFile> smallFiles) {
    if (this.config.isClusteringEnabled()) {
      return smallFiles.stream()
          .filter(smallFile -> !pendingClusteringFileGroupsId.contains(smallFile.location.getFileId())).collect(Collectors.toList());
    } else {
      return smallFiles;
    }
  }

  private void assignInserts(WorkloadProfile profile, HoodieEngineContext context) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    Set<String> partitionPaths = profile.getPartitionPaths();

    // 获取历史平均record大小 size
    long averageRecordSize =
        averageBytesPerRecord(table.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
            config);
    LOG.info("AvgRecordSize => " + averageRecordSize);

    Map<String, List<SmallFile>> partitionSmallFilesMap =
        getSmallFilesForPartitions(new ArrayList<String>(partitionPaths), context);

    // Get the in pending clustering fileId for each partition path.
    // 获取每一个partition中处于pending clustering状态的fileID
    Map<String, Set<String>> partitionPathToPendingClusteringFileGroupsId = getPartitionPathToPendingClusteringFileGroupsId();

    // per partition 去做处理
    for (String partitionPath : partitionPaths) {
      WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
      if (pStat.getNumInserts() > 0) {

        // 从partitionSmallFilesMap中去掉所有pending clustering的文件
        // 获取最终所有的小文件 fileIDs
        List<SmallFile> smallFiles =
            filterSmallFilesInClustering(partitionPathToPendingClusteringFileGroupsId.getOrDefault(partitionPath, Collections.emptySet()),
                partitionSmallFilesMap.get(partitionPath));

        this.smallFiles.addAll(smallFiles);

        LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

        long totalUnassignedInserts = pStat.getNumInserts();
        // 二者顺序有对应关系 bucketNumber 以及当前bucketNumber下要ingest的数据量
        List<Integer> bucketNumbers = new ArrayList<>();
        List<Long> recordsPerBucket = new ArrayList<>();

        // first try packing this into one of the smallFiles
        // 遍历所有smallFiles till totalUnassignedInserts 分配完 或者 small files 遍历完
        // TODO 现在是可劲儿写一个小文件
        // TODO 最好是能修改成所有小文件并发写
        for (SmallFile smallFile : smallFiles) {
          // 对于每个小文件
          // 计算当前小文件能够append的最大records条数，上限为当前分区的totalUnassignedInserts
          long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
              totalUnassignedInserts);
          if (recordsToAppend > 0 && totalUnassignedInserts > 0) {
            // create a new bucket or re-use an existing bucket
            int bucket;

            // 如果要insert的small file同样会涉及到update 则直接获取 当前小文件ID对应的bucket编号 并填写bucketNumbers以及recordsPerBucket
            if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
              bucket = updateLocationToBucket.get(smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
            } else {
              // 如果当前小文件不涉及update
              // 那么将添加一个insert视为update 去append小文件
              // 对insert 新建一个update bucket 将部分insert转换为对current small file 的append
              // 此处会对 totalBuckets++
              bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
            }
            bucketNumbers.add(bucket);
            recordsPerBucket.add(recordsToAppend);
            totalUnassignedInserts -= recordsToAppend;
          }
        }

        // if we have anything more, create new insert buckets, like normal
        // 如果小文件分配后 仍有Unassigned Inserts records 或者 第一次 ingest 当前分区的数据
        // 则新建bucket 做insert操作
        if (totalUnassignedInserts > 0) {

          // 获取 hoodie.copyonwrite.insert.split.size 默认值为500,000
          long insertRecordsPerBucket = config.getCopyOnWriteInsertSplitSize();
          // hoodie.copyonwrite.insert.auto.split 默认为 true
          if (config.shouldAutoTuneInsertSplits()) {
            // hoodie.parquet.max.file.size 默认120Mb
            // 根据averageRecordSize 算出insertRecordsPerBucket 每一个bucket需要ingest的record数量
            insertRecordsPerBucket = config.getParquetMaxFileSize() / averageRecordSize;
          }

          // 计算需要新建几个insert bucket 总共所有未分配的records数/每个bucket应该ingest的records数量(用户指定或自动计算)
          // TODO 用户在这里直接指定insertBuckets 从而控制init 文件数量 以及partition数量即并发度
          // TODO 现在的workaround是 1.关闭 hoodie.copyonwrite.insert.auto.split 2. 将insertRecordsPerBucket设置为一个比较小的值
          // TODO workaround的缺点在于需要预估totalUnassignedInserts大小，也就是说控制的不够精确
          // TODO 或者 disable avgRecordSize计算 并指定一个较大的avg值 使得
          int insertBuckets = (int) Math.ceil((1.0 * totalUnassignedInserts) / insertRecordsPerBucket);
          LOG.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
              + ", totalInsertBuckets => " + insertBuckets + ", recordsPerBucket => " + insertRecordsPerBucket);
          for (int b = 0; b < insertBuckets; b++) {
            bucketNumbers.add(totalBuckets);
            if (b < insertBuckets - 1) {
              recordsPerBucket.add(insertRecordsPerBucket);
            } else {
              // 最后一批数据
              recordsPerBucket.add(totalUnassignedInserts - (insertBuckets - 1) * insertRecordsPerBucket);
            }
            BucketInfo bucketInfo = new BucketInfo(BucketType.INSERT, FSUtils.createNewFileIdPfx(), partitionPath);
            bucketInfoMap.put(totalBuckets, bucketInfo);
            totalBuckets++;
          }
        }

        // 至此所有input records已经分配完毕
        // 这里很重要
        // Go over all such buckets, and assign weights as per amount of incoming inserts.
        List<InsertBucketCumulativeWeightPair> insertBuckets = new ArrayList<>();
        double curentCumulativeWeight = 0;
        // 遍历所有bucket编号
        for (int i = 0; i < bucketNumbers.size(); i++) {
          InsertBucket bkt = new InsertBucket();
          bkt.bucketNumber = bucketNumbers.get(i);
          // 计算当前bucket下ingest record to append 与 total ingest records 数量的占比
          bkt.weight = (1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts();
          curentCumulativeWeight += bkt.weight;
          insertBuckets.add(new InsertBucketCumulativeWeightPair(bkt, curentCumulativeWeight));
        }
        LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
        partitionPathToInsertBucketInfos.put(partitionPath, insertBuckets);
      }
    }
  }

  private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths, HoodieEngineContext context) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    Map<String, List<SmallFile>> partitionSmallFilesMap = new HashMap<>();
    if (partitionPaths != null && partitionPaths.size() > 0) {
      context.setJobStatus(this.getClass().getSimpleName(), "Getting small files from partitions");
      // 此处看到 会trigger一个job 去探知当前input records中涉及到的partition中的small file的数量
      // 这个job的并发度等于cover的partition数量
      JavaRDD<String> partitionPathRdds = jsc.parallelize(partitionPaths, partitionPaths.size());
      partitionSmallFilesMap = partitionPathRdds.mapToPair((PairFunction<String, String, List<SmallFile>>)
          partitionPath -> new Tuple2<>(partitionPath, getSmallFiles(partitionPath))).collectAsMap();
    }

    return partitionSmallFilesMap;
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  protected List<SmallFile> getSmallFiles(String partitionPath) {

    // smallFiles only for partitionPath
    List<SmallFile> smallFileLocations = new ArrayList<>();

    HoodieTimeline commitTimeline = table.getMetaClient().getCommitsTimeline().filterCompletedInstants();

    if (!commitTimeline.empty()) { // if we have some commits
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      List<HoodieBaseFile> allFiles = table.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        if (file.getFileSize() < config.getParquetSmallFileLimit()) {
          String filename = file.getFileName();
          SmallFile sf = new SmallFile();
          sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
          sf.sizeBytes = file.getFileSize();
          smallFileLocations.add(sf);
        }
      }
    }

    return smallFileLocations;
  }

  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoMap.get(bucketNumber);
  }

  public List<InsertBucketCumulativeWeightPair> getInsertBuckets(String partitionPath) {
    return partitionPathToInsertBucketInfos.get(partitionPath);
  }

  @Override
  public int numPartitions() {
    return totalBuckets;
  }

  // key => [HoodieKey, HoodieRecordLocation], comes from HoodieRecord
  // return bucketNumber
  @Override
  public int getPartition(Object key) {
    // 挑选bucket
    // pick up bucket for key
    Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
        (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
    if (keyLocation._2().isPresent()) {
      // 如果当前key是update，则直接返回相对应的bucket number
      HoodieRecordLocation location = keyLocation._2().get();
      return updateLocationToBucket.get(location.getFileId());
    } else {
      // 对于insert records
      String partitionPath = keyLocation._1().getPartitionPath();
      List<InsertBucketCumulativeWeightPair> targetBuckets = partitionPathToInsertBucketInfos.get(partitionPath);
      // pick the target bucket to use based on the weights.
      final long totalInserts = Math.max(1, profile.getWorkloadStat(partitionPath).getNumInserts());

      //TODO MD5 性能可以吗？看下火焰图
      final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
      final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;

      // 如果找到关键字，则返回值为关键字在数组中的位置索引，且索引从0开始
      // 如果没有找到关键字，返回值为负的插入点值，所谓插入点值就是第一个比关键字大的元素在数组中的位置索引，而且这个位置索引从1开始。
      int index = Collections.binarySearch(targetBuckets, new InsertBucketCumulativeWeightPair(new InsertBucket(), r));

      if (index >= 0) {
        return targetBuckets.get(index).getKey().bucketNumber;
      }

      if ((-1 * index - 1) < targetBuckets.size()) {
        return targetBuckets.get((-1 * index - 1)).getKey().bucketNumber;
      }

      // return first one, by default
      return targetBuckets.get(0).getKey().bucketNumber;
    }
  }

  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  protected static long averageBytesPerRecord(HoodieTimeline commitTimeline, HoodieWriteConfig hoodieWriteConfig) {
    long avgSize = hoodieWriteConfig.getCopyOnWriteRecordSizeEstimate();
    long fileSizeThreshold = (long) (hoodieWriteConfig.getRecordSizeEstimationThreshold() * hoodieWriteConfig.getParquetSmallFileLimit());
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
          long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
          if (totalBytesWritten > fileSizeThreshold && totalRecordsWritten > 0) {
            avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
            break;
          }
        }
      }
    } catch (Throwable t) {
      // make this fail safe.
      LOG.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }
}
