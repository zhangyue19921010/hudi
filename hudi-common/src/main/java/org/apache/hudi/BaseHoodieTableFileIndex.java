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

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieBaseFileWithFileStatus;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.CachingPath.createPathUnsafe;

/**
 * Common (engine-agnostic) File Index implementation enabling individual query engines to
 * list Hudi Table contents based on the
 *
 * <ul>
 *   <li>Table type (MOR, COW)</li>
 *   <li>Query type (snapshot, read_optimized, incremental)</li>
 *   <li>Query instant/range</li>
 * </ul>
 */
public abstract class BaseHoodieTableFileIndex implements AutoCloseable {
  private static final Logger LOG = LogManager.getLogger(BaseHoodieTableFileIndex.class);

  private final String[] partitionColumns;

  protected final HoodieMetadataConfig metadataConfig;

  private final HoodieTableQueryType queryType;
  private final Option<String> specifiedQueryInstant;
  protected final List<Path> queryPaths;

  private final boolean shouldIncludePendingCommits;
  private final boolean shouldValidateInstant;

  private final HoodieTableType tableType;
  protected final Path basePath;

  private final HoodieTableMetaClient metaClient;
  private final HoodieEngineContext engineContext;

  private final TypedProperties configProperties;

  private final transient FileStatusCache fileStatusCache;

  protected transient volatile long cachedFileSize = 0L;
  protected transient volatile Map<PartitionPath, List<FileSlice>> cachedAllInputFileSlices;

  protected volatile boolean queryAsNonePartitionedTable = false;

  private transient volatile HoodieTableFileSystemView fileSystemView = null;

  private transient HoodieTableMetadata tableMetadata = null;

  /**
   * @param engineContext Hudi engine-specific context
   * @param metaClient Hudi table's meta-client
   * @param configProperties unifying configuration (in the form of generic properties)
   * @param queryType target query type
   * @param queryPaths target DFS paths being queried
   * @param specifiedQueryInstant instant as of which table is being queried
   * @param shouldIncludePendingCommits flags whether file-index should exclude any pending operations
   * @param shouldValidateInstant flags to validate whether query instant is present in the timeline
   * @param fileStatusCache transient cache of fetched [[FileStatus]]es
   */
  public BaseHoodieTableFileIndex(HoodieEngineContext engineContext,
                                  HoodieTableMetaClient metaClient,
                                  TypedProperties configProperties,
                                  HoodieTableQueryType queryType,
                                  List<Path> queryPaths,
                                  Option<String> specifiedQueryInstant,
                                  boolean shouldIncludePendingCommits,
                                  boolean shouldValidateInstant,
                                  FileStatusCache fileStatusCache) {
    this.configProperties = configProperties;
    this.partitionColumns = metaClient.getTableConfig().getPartitionFields()
        .orElse(new String[0]);

    this.metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(configProperties)
        .build();

    this.queryType = queryType;
    this.queryPaths = queryPaths;
    this.specifiedQueryInstant = specifiedQueryInstant;
    this.shouldIncludePendingCommits = shouldIncludePendingCommits;
    this.shouldValidateInstant = shouldValidateInstant;

    this.tableType = metaClient.getTableType();
    this.basePath = metaClient.getBasePathV2();

    this.metaClient = metaClient;
    this.engineContext = engineContext;
    this.fileStatusCache = fileStatusCache;

    doRefresh();
  }

  protected abstract Object[] parsePartitionColumnValues(String[] partitionColumns, String partitionPath);

  /**
   * Returns latest completed instant as seen by this instance of the file-index
   */
  public Option<HoodieInstant> getLatestCompletedInstant() {
    return getActiveTimeline().filterCompletedInstants().lastInstant();
  }

  /**
   * Returns table's base-path
   */
  public String getBasePath() {
    return basePath.toString();
  }

  /**
   * Fetch list of latest base files and log files per partition.
   *
   * @return mapping from string partition paths to its base/log files
   */
  public Map<String, List<FileSlice>> listFileSlices() {
    return cachedAllInputFileSlices.entrySet()
        .stream()
        .collect(Collectors.toMap(e -> e.getKey().path, Map.Entry::getValue));
  }

  public int getFileSlicesCount() {
    return cachedAllInputFileSlices.values().stream()
        .mapToInt(List::size).sum();
  }

  @Override
  public void close() throws Exception {
    resetTableMetadata(null);
  }

  protected List<PartitionPath> getAllQueryPartitionPaths() {
    List<String> queryRelativePartitionPaths = queryPaths.stream()
        .map(path -> FSUtils.getRelativePartitionPath(basePath, path))
        .filter(relatedPath -> !StringUtils.isNullOrEmpty(relatedPath))
        .collect(Collectors.toList());

    List<String> allPartitionPathsUnchecked = getAllPartitionPathsUnchecked();

    // Load all the partition path from the basePath, and filter by the query partition path.
    // TODO load files from the queryRelativePartitionPaths directly.
    List<String> matchedPartitionPaths;
    if (!queryRelativePartitionPaths.isEmpty()) {
      matchedPartitionPaths = allPartitionPathsUnchecked
          .stream()
          .filter(path -> queryRelativePartitionPaths.stream().anyMatch(path::startsWith))
          .collect(Collectors.toList());
    } else {
      matchedPartitionPaths = allPartitionPathsUnchecked;
    }

    // Convert partition's path into partition descriptor
    return matchedPartitionPaths.stream()
        .map(partitionPath -> {
          Object[] partitionColumnValues = parsePartitionColumnValues(partitionColumns, partitionPath);
          return new PartitionPath(partitionPath, partitionColumnValues);
        })
        .collect(Collectors.toList());
  }

  protected void refresh() {
    fileStatusCache.invalidate();
    doRefresh();
  }

  protected HoodieTimeline getActiveTimeline() {
    // NOTE: We have to use commits and compactions timeline, to make sure that we're properly
    //       handling the following case: when records are inserted into the new log-file w/in the file-group
    //       that is under the pending compaction process, new log-file will bear the compaction's instant (on the
    //       timeline) in its name, as opposed to the base-file's commit instant. To make sure we're not filtering
    //       such log-file we have to _always_ include pending compaction instants into consideration
    // TODO(HUDI-3302) re-evaluate whether we should filter any commits in here
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline();
    if (shouldIncludePendingCommits) {
      return timeline;
    } else {
      return timeline.filterCompletedAndCompactionInstants();
    }
  }

  /**
   * Load all partition paths and it's files under the query table path.
   */
  private Map<PartitionPath, FileStatus[]> loadPartitionPathFiles() {
    // List files in all partition paths
    List<PartitionPath> pathToFetch = new ArrayList<>();
    Map<PartitionPath, FileStatus[]> cachedPartitionToFiles = new HashMap<>();

    // Fetch from the FileStatusCache
    List<PartitionPath> partitionPaths = getAllQueryPartitionPaths();
    partitionPaths.forEach(partitionPath -> {
      Option<FileStatus[]> filesInPartition = fileStatusCache.get(partitionPath.fullPartitionPath(basePath));
      if (filesInPartition.isPresent()) {
        cachedPartitionToFiles.put(partitionPath, filesInPartition.get());
      } else {
        pathToFetch.add(partitionPath);
      }
    });

    Map<PartitionPath, FileStatus[]> fetchedPartitionToFiles;

    if (pathToFetch.isEmpty()) {
      fetchedPartitionToFiles = Collections.emptyMap();
    } else {
      Map<String, PartitionPath> fullPartitionPathsMapToFetch = pathToFetch.stream()
          .collect(Collectors.toMap(
              partitionPath -> partitionPath.fullPartitionPath(basePath).toString(),
              Function.identity())
          );

      fetchedPartitionToFiles =
          getAllFilesInPartitionsUnchecked(fullPartitionPathsMapToFetch.keySet())
              .entrySet()
              .stream()
              .collect(Collectors.toMap(e -> fullPartitionPathsMapToFetch.get(e.getKey()), e -> e.getValue()));

    }

    // Update the fileStatusCache
    fetchedPartitionToFiles.forEach((partitionPath, filesInPartition) -> {
      fileStatusCache.put(partitionPath.fullPartitionPath(basePath), filesInPartition);
    });

    return CollectionUtils.combine(cachedPartitionToFiles, fetchedPartitionToFiles);
  }

  private void doRefresh() {
    long startTime = System.currentTimeMillis();

    HoodieTableMetadata newTableMetadata = HoodieTableMetadata.create(engineContext, metadataConfig, basePath.toString(),
        FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue());

    resetTableMetadata(newTableMetadata);

    Map<PartitionPath, FileStatus[]> partitionFiles = loadPartitionPathFiles();

    metaClient.reloadActiveTimeline();

    HoodieTimeline activeTimeline = getActiveTimeline();
    Option<HoodieInstant> latestInstant = activeTimeline.lastInstant();

    Option<String> queryInstant =
        specifiedQueryInstant.or(() -> latestInstant.map(HoodieInstant::getTimestamp));

    validate(activeTimeline, queryInstant);

    int parallelism = Integer.parseInt(String.valueOf(configProperties.getOrDefault(HoodieCommonConfig.TABLE_LOADING_PARALLELISM.key(),
        HoodieCommonConfig.TABLE_LOADING_PARALLELISM.defaultValue())));

    String mode = String.valueOf(configProperties.getOrDefault(HoodieCommonConfig.TABLE_LOADING_MODE.key(),
        HoodieCommonConfig.TABLE_LOADING_MODE.defaultValue()));

    long buildCacheFileSlicesLocalStart = System.currentTimeMillis();
    if (parallelism <= 0 || partitionFiles.size() == 0) {
      cachedAllInputFileSlices = buildCacheFileSlicesLocal(partitionFiles, activeTimeline, queryInstant);
    } else if (mode.equalsIgnoreCase("CLUSTER")) {
      cachedAllInputFileSlices = buildCacheFileSlicesClusterParallel(parallelism, partitionFiles, activeTimeline, queryInstant);
    } else {
      cachedAllInputFileSlices = buildCacheFileSlicesLocalParallel(parallelism, partitionFiles, activeTimeline, queryInstant);
    }
    long buildCacheFileSlicesLocalEnd = System.currentTimeMillis();
    LOG.info(String.format("Build cache file slices, spent: %d ms", buildCacheFileSlicesLocalEnd - buildCacheFileSlicesLocalStart));

    cachedFileSize = cachedAllInputFileSlices.values().stream()
        .flatMap(Collection::stream)
        .mapToLong(BaseHoodieTableFileIndex::fileSliceSize)
        .sum();

    // If the partition value contains InternalRow.empty, we query it as a non-partitioned table.
    queryAsNonePartitionedTable = partitionFiles.keySet().stream().anyMatch(p -> p.values.length == 0);

    long duration = System.currentTimeMillis() - startTime;

    LOG.info(String.format("Refresh table %s, spent: %d ms", metaClient.getTableConfig().getTableName(), duration));
  }

  private Map<PartitionPath, List<FileSlice>> buildCacheFileSlicesLocal(Map<PartitionPath, FileStatus[]> partitionFiles, HoodieTimeline activeTimeline,
                                                                        Option<String> queryInstant) {
    FileStatus[] allFiles = partitionFiles.values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);
    this.fileSystemView = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles);

    return getCandidateFileSlices(partitionFiles, queryInstant, fileSystemView);
  }

  private Map<PartitionPath, List<FileSlice>> buildCacheFileSlicesLocalParallel(int parallelism, Map<PartitionPath, FileStatus[]> partitionFiles,
                                                                                HoodieTimeline activeTimeline, Option<String> queryInstant) {
    HashMap<PartitionPath, List<FileSlice>> res = new HashMap<>();
    parallelism = Math.max(1, Math.min(parallelism, partitionFiles.size()));
    int totalPartitions = partitionFiles.size();
    int cursor = 0;
    int step = totalPartitions / parallelism;
    ArrayList<Map.Entry<PartitionPath, FileStatus[]>> entryList = new ArrayList<>(partitionFiles.entrySet());
    ExecutorService pool =  Executors.newFixedThreadPool((parallelism + 1));
    ArrayList<CompletableFuture<Map<PartitionPath, List<FileSlice>>>> futureList = new ArrayList<>(parallelism + 1);

    while (cursor + step <= totalPartitions) {
      List<Map.Entry<PartitionPath, FileStatus[]>> innerList = entryList.subList(cursor, cursor + step);
      CompletableFuture<Map<PartitionPath, List<FileSlice>>> innerFuture = computeCacheFileSlices(innerList, pool, activeTimeline, queryInstant);
      futureList.add(innerFuture);
      cursor = cursor + step;
    }

    if (cursor < totalPartitions) {
      List<Map.Entry<PartitionPath, FileStatus[]>> finalList = entryList.subList(cursor, totalPartitions);
      CompletableFuture<Map<PartitionPath, List<FileSlice>>> finalFuture = computeCacheFileSlices(finalList, pool, activeTimeline, queryInstant);
      futureList.add(finalFuture);
    }

    CompletableFuture<List<Map<PartitionPath, List<FileSlice>>>> waiter = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
        .thenApply(v -> futureList.stream().map(CompletableFuture::join).collect(Collectors.toList()));
    waiter.join().forEach(res::putAll);
    pool.shutdown();

    return res;
  }

  private CompletableFuture<Map<PartitionPath, List<FileSlice>>> computeCacheFileSlices(List<Map.Entry<PartitionPath, FileStatus[]>> innerList, ExecutorService pool,
                                                                                        HoodieTimeline activeTimeline, Option<String> queryInstant) {
    return CompletableFuture.supplyAsync(() -> {
      HashMap<PartitionPath, FileStatus[]> innerPartitionFiles = new HashMap<>();
      innerList.forEach(entry -> {
        PartitionPath innerPartitionPath = entry.getKey();
        FileStatus[] innerFileStatuses = entry.getValue();
        innerPartitionFiles.put(innerPartitionPath, innerFileStatuses);
      });

      FileStatus[] allFiles = innerPartitionFiles.values().stream().flatMap(Arrays::stream).toArray(FileStatus[]::new);
      HoodieTableFileSystemView fileSystemViewInternal = new HoodieTableFileSystemView(metaClient, activeTimeline, allFiles);
      return getCandidateFileSlices(innerPartitionFiles, queryInstant, fileSystemViewInternal);
    }, pool);
  }

  private Map<PartitionPath, List<FileSlice>> getCandidateFileSlices(Map<PartitionPath, FileStatus[]> innerPartitionFiles, Option<String> queryInstant,
                                                                     HoodieTableFileSystemView fileSystemView) {
    // NOTE: For MOR table, when the compaction is inflight, we need to not only fetch the
    // latest slices, but also include the base and log files of the second-last version of
    // the file slice in the same file group as the latest file slice that is under compaction.
    // This logic is realized by `AbstractTableFileSystemView::getLatestMergedFileSlicesBeforeOrOn`
    // API.  Note that for COW table, the merging logic of two slices does not happen as there
    // is no compaction, thus there is no performance impact.
    return innerPartitionFiles.keySet().stream()
        .collect(Collectors.toMap(
            Function.identity(),
            partitionPath ->
                queryInstant.map(instant ->
                    fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partitionPath.path, queryInstant.get())
                )
                    .orElse(fileSystemView.getLatestFileSlices(partitionPath.path))
                    .collect(Collectors.toList())
            )
        );
  }

  private Map<PartitionPath, List<FileSlice>> buildCacheFileSlicesClusterParallel(int parallelism, Map<PartitionPath, FileStatus[]> partitionFiles,
                                                                                  HoodieTimeline activeTimeline, Option<String> queryInstant) {
    parallelism = Math.max(1, Math.min(parallelism, partitionFiles.size()));

    HoodieTableMetaClient metaClientInternal = metaClient;

    return engineContext.mapPartitionsToPair(partitionFiles.entrySet().stream(), iterator -> {
      ArrayList<FileStatus> allFilesInternal = new ArrayList<>();
      ArrayList<PartitionPath> partitionPaths = new ArrayList<>();

      iterator.forEachRemaining(entry -> {
        allFilesInternal.addAll(Arrays.asList(entry.getValue()));
        partitionPaths.add(entry.getKey());
      });

      HoodieTableFileSystemView fileSystemViewInternal = new HoodieTableFileSystemView(metaClientInternal, activeTimeline, allFilesInternal.toArray(new FileStatus[0]));

      return partitionPaths.stream().map(partitionPath -> {
        List<FileSlice> filesSlices = queryInstant.map(instant ->
            fileSystemViewInternal.getLatestMergedFileSlicesBeforeOrOn(partitionPath.path, queryInstant.get())
        )
            .orElse(fileSystemViewInternal.getLatestFileSlices(partitionPath.path))
            .map(fileSlice -> {

              // convert HoodieBaseFile/HoodieLogFile to HoodieBaseFileWithFileStatus/HoodieLogFileWithFileStatus if necessary
              // bcz ori one's FileStatus is transient.
              if (!fileSlice.isEmpty()) {

                HoodieBaseFile hoodieBaseFile = fileSlice.getBaseFile().isPresent() ? new HoodieBaseFileWithFileStatus(fileSlice.getBaseFile().get()) : null;
                return new FileSlice(fileSlice.getFileGroupId(), fileSlice.getBaseInstantTime(), hoodieBaseFile, fileSlice.getRawLogFiles());
              } else {
                return fileSlice;
              }
            })
            .collect(Collectors.toList());
        return Pair.of(partitionPath, filesSlices);
      });
    }, parallelism).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private Map<String, FileStatus[]> getAllFilesInPartitionsUnchecked(Collection<String> fullPartitionPathsMapToFetch) {
    try {
      return tableMetadata.getAllFilesInPartitions(new ArrayList<>(fullPartitionPathsMapToFetch));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to list partition paths for a table", e);
    }
  }

  private List<String> getAllPartitionPathsUnchecked() {
    try {
      return isPartitionedTable() ? tableMetadata.getAllPartitionPaths() : Collections.singletonList("");
    } catch (IOException e) {
      throw new HoodieIOException("Failed to fetch partition paths for a table", e);
    }
  }

  private void validate(HoodieTimeline activeTimeline, Option<String> queryInstant) {
    if (shouldValidateInstant) {
      if (queryInstant.isPresent() && !activeTimeline.containsInstant(queryInstant.get())) {
        throw new HoodieIOException(String.format("Query instant (%s) not found in the timeline", queryInstant.get()));
      }
    }
  }

  private static long fileSliceSize(FileSlice fileSlice) {
    long logFileSize = fileSlice.getLogFiles().map(HoodieLogFile::getFileSize)
        .filter(s -> s > 0)
        .reduce(0L, Long::sum);

    return fileSlice.getBaseFile().map(BaseFile::getFileLen).orElse(0L) + logFileSize;
  }

  private void resetTableMetadata(HoodieTableMetadata newTableMetadata) {
    if (tableMetadata != null) {
      try {
        tableMetadata.close();
      } catch (Exception e) {
        throw new HoodieException("Failed to close HoodieTableMetadata instance", e);
      }
    }
    tableMetadata = newTableMetadata;
  }

  private boolean isPartitionedTable() {
    return partitionColumns.length > 0 || HoodieTableMetadata.isMetadataTable(basePath.toString());
  }

  public static final class PartitionPath {

    final String path;
    final Object[] values;

    public PartitionPath(String path, Object[] values) {
      this.path = path;
      this.values = values;
    }

    public String getPath() {
      return path;
    }

    Path fullPartitionPath(Path basePath) {
      if (!path.isEmpty()) {
        // NOTE: Since we now that the path is a proper relative path that doesn't require
        //       normalization we create Hadoop's Path using more performant unsafe variant
        return new CachingPath(basePath, createPathUnsafe(path));
      }

      return basePath;
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof PartitionPath
          && Objects.equals(path, ((PartitionPath) other).path)
          && Arrays.equals(values, ((PartitionPath) other).values);
    }

    @Override
    public int hashCode() {
      return path.hashCode() * 1103 + Arrays.hashCode(values);
    }
  }

  protected interface FileStatusCache {
    Option<FileStatus[]> get(Path path);

    void put(Path path, FileStatus[] leafFiles);

    void invalidate();
  }
}
