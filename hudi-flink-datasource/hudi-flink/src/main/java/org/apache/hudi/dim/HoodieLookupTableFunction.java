/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.dim;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.source.FileIndex;
import org.apache.hudi.source.IncrementalInputSplits;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.cow.CopyOnWriteInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.table.format.mor.MergeOnReadInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.AvroSchemaConverter;
import org.apache.hudi.util.InputFormats;
import org.apache.hudi.util.StreamerUtil;

import org.apache.avro.Schema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.configuration.HadoopConfigurations.getParquetConf;

/**
 * Hoodie lookup table source that always read the latest snapshot of the underneath table.
 */
public class HoodieLookupTableFunction extends TableFunction<RowData> {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieLookupTableFunction.class);

  // cache for lookup data
  private transient Map<RowData, List<RowData>> cache;

  // timestamp when cache expires
  private transient long nextLoadTime;

  private transient InputFormat<RowData, InputSplit> format;

  private transient Iterator<InputSplit> splitIterator;

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(10);

  private static final int MAX_RETRIES = 3;

  private static final int NO_LIMIT_CONSTANT = -1;

  private volatile boolean isRunning = true;

  private transient TypeSerializer<RowData> serializer;

  private transient RowData.FieldGetter[] lookupFieldGetters;

  private transient RuntimeContext runtimeContext;

  private final Duration reloadInterval;

  private final RowType typeInfo;

  private final int[] lookupKeys;

  private final Configuration conf;

  private final RowType tableRowType;

  private final long maxCompactionMemoryInBytes;

  private final int[] requiredPos;

  //private final ResolvedSchema schema;

  private final long limit;

  //private final List<ResolvedExpression> filters;

  private final List<Map<String, String>> requiredPartitions;

  private final List<String> partitionKeys;

  private final Map<String, String> hadoopMap;

  private final String[] schemaFieldNames;

  private final DataType[] schemaFieldTypes;

  private final InternalSchemaManager internalSchemaManager;

  private transient org.apache.hadoop.conf.Configuration hadoopConf;

  private transient HoodieTableMetaClient metaClient;

  private transient Path path;

  private transient FileIndex fileIndex;

  private transient HoodieInstant currentCommit;

  public HoodieLookupTableFunction(Duration reloadInterval, RowType typeInfo, int[] lookupKeys, Configuration conf, RowType tableRowType, long maxCompactionMemoryInBytes, int[] requiredPos,
                                   long limit, List<Map<String, String>> requiredPartitions, List<String> partitionKeys, Map<String, String> hadoopMap, String[] schemaFieldNames,
                                   DataType[] schemaFieldTypes, InternalSchemaManager internalSchemaManager) {
    this.reloadInterval = reloadInterval;
    this.typeInfo = typeInfo;
    this.lookupKeys = lookupKeys;
    this.conf = conf;
    this.tableRowType = tableRowType;
    this.maxCompactionMemoryInBytes = maxCompactionMemoryInBytes;
    this.requiredPos = requiredPos;
    this.limit = limit;
    this.requiredPartitions = requiredPartitions;
    this.partitionKeys = partitionKeys;
    this.hadoopMap = hadoopMap;
    this.schemaFieldTypes = schemaFieldTypes;
    this.schemaFieldNames = schemaFieldNames;
    this.internalSchemaManager = internalSchemaManager == null
            ? InternalSchemaManager.get(this.conf, this.metaClient)
            : internalSchemaManager;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);

    this.path = new Path(conf.getString(FlinkOptions.PATH));
    this.hadoopConf = new org.apache.hadoop.conf.Configuration();
    this.fileIndex = FileIndex.instance(this.path, this.conf, this.tableRowType);
    for (Map.Entry<String, String> entry : hadoopMap.entrySet()) {
      hadoopConf.set(entry.getKey(), entry.getValue());
    }
    this.metaClient = StreamerUtil.metaClientForReader(conf, hadoopConf);
    format = (InputFormat<RowData, InputSplit>) getBatchInputFormat();
    Field field = FunctionContext.class.getDeclaredField("context");
    field.setAccessible(true);
    runtimeContext = (RuntimeContext) field.get(context);
    if (this.format instanceof RichInputFormat) {
      ((RichInputFormat) this.format).setRuntimeContext(runtimeContext);
    }
    cache = new HashMap<>();
    nextLoadTime = -1L;
    this.serializer = InternalSerializers.create(typeInfo);
    this.lookupFieldGetters = new RowData.FieldGetter[lookupKeys.length];
    for (int i = 0; i < lookupKeys.length; i++) {
      lookupFieldGetters[i] =
          RowData.createFieldGetter(typeInfo.getTypeAt(lookupKeys[i]), lookupKeys[i]);
    }
  }

  public void eval(Object... values) {
    checkCacheReload();
    RowData lookupKey = GenericRowData.of(values);
    List<RowData> matchedRows = cache.get(lookupKey);
    if (matchedRows != null) {
      for (RowData matchedRow : matchedRows) {
        collect(matchedRow);
      }
    }
  }

  private void checkCacheReload() {
    if (nextLoadTime > System.currentTimeMillis()) {
      return;
    }
    if (nextLoadTime > 0) {
      LOG.info(
          "Lookup join cache has expired after {} minute(s), reloading",
          reloadInterval.toMinutes());
    } else {
      LOG.info("Populating lookup join cache");
    }
    doReload();
  }

  private void doReload() {
    int numRetry = 0;
    while (true) {
      try {
        HoodieActiveTimeline latestCommit = metaClient.reloadActiveTimeline();
        Option<HoodieInstant> latestCommitInstant = latestCommit.getCommitsTimeline().lastInstant();
        if (latestCommit.empty()) {
          LOG.info("No commit instant found currently.");
          return;
        }
        // Determine whether to reload data by comparing instant
        if (currentCommit != null && latestCommitInstant.get().equals(currentCommit)) {
          LOG.info("Ignore loading data because the commit instant " + currentCommit + " has not changed.");
          return;
        }

        cache.clear();

        format = (InputFormat<RowData, InputSplit>) getBatchInputFormat();
        if (this.isRunning && this.format instanceof RichInputFormat) {
          ((RichInputFormat) this.format).openInputFormat();
        }

        RowData nextElement = this.serializer.createInstance();

        splitIterator = Arrays.stream(format.createInputSplits(1)).iterator();

        while (this.isRunning) {
          this.format.open(this.splitIterator.next());

          while (this.isRunning && !this.format.reachedEnd()) {
            nextElement = this.format.nextRecord(nextElement);
            if (nextElement != null) {
              RowData key = extractLookupKey(nextElement);
              List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
              rows.add(serializer.copy(nextElement));
            } else {
              break;
            }
          }

          this.format.close();
          if (this.format instanceof RichInputFormat) {
            ((RichInputFormat) this.format).closeInputFormat();
          }

          if (this.isRunning) {
            this.isRunning = this.splitIterator.hasNext();
          }
        }
        LOG.info("HoodieLookupTable finish to cache data and size is " + cache.size());
        nextLoadTime = System.currentTimeMillis() + reloadInterval.toMillis();
        isRunning = true;
        currentCommit = latestCommitInstant.get();
        return;
      } catch (Exception e) {
        if (numRetry >= MAX_RETRIES) {
          throw new FlinkRuntimeException(
              String.format(
                  "Failed to load table into cache after %d retries", numRetry),
              e);
        }
        numRetry++;
        long toSleep = numRetry * RETRY_INTERVAL.toMillis();
        LOG.warn(
            "Failed to load table into cache, will retry in {} milliseconds",
            toSleep,
            e);
        try {
          Thread.sleep(toSleep);
        } catch (InterruptedException ex) {
          LOG.warn("Interrupted while waiting to retry failed cache load, aborting", ex);
          throw new FlinkRuntimeException(ex);
        }
      }
    }
  }

  private RowData extractLookupKey(RowData row) {
    GenericRowData key = new GenericRowData(lookupFieldGetters.length);
    for (int i = 0; i < lookupFieldGetters.length; i++) {
      key.setField(i, lookupFieldGetters[i].getFieldOrNull(row));
    }
    return key;
  }

  // The method from HoodieTableSource class
  private InputFormat<RowData, ?> getBatchInputFormat() {
    final Schema tableAvroSchema = getTableAvroSchema();
    final DataType rowDataType = AvroSchemaConverter.convertToDataType(tableAvroSchema);
    final RowType rowType = (RowType) rowDataType.getLogicalType();
    final RowType requiredRowType = (RowType) getProducedDataType().notNull().getLogicalType();

    final String queryType = this.conf.getString(FlinkOptions.QUERY_TYPE);
    switch (queryType) {
      case FlinkOptions.QUERY_TYPE_SNAPSHOT:
        final HoodieTableType tableType = HoodieTableType.valueOf(this.conf.getString(FlinkOptions.TABLE_TYPE));
        switch (tableType) {
          case MERGE_ON_READ:
            final List<MergeOnReadInputSplit> inputSplits = buildFileIndex();
            if (inputSplits.size() == 0) {
              // When there is no input splits, just return an empty source.
              LOG.warn("No input splits generate for MERGE_ON_READ input format, returns empty collection instead");
              return InputFormats.EMPTY_INPUT_FORMAT;
            }
            return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
                rowDataType, inputSplits, false);
          case COPY_ON_WRITE:
            return baseFileOnlyInputFormat();
          default:
            throw new HoodieException("Unexpected table type: " + this.conf.getString(FlinkOptions.TABLE_TYPE));
        }
      case FlinkOptions.QUERY_TYPE_READ_OPTIMIZED:
        return baseFileOnlyInputFormat();
      case FlinkOptions.QUERY_TYPE_INCREMENTAL:
        IncrementalInputSplits incrementalInputSplits = IncrementalInputSplits.builder()
            .conf(conf)
            .path(FilePathUtils.toFlinkPath(path))
            .rowType(this.tableRowType)
            .maxCompactionMemoryInBytes(maxCompactionMemoryInBytes)
            .requiredPartitions(getRequiredPartitionPaths()).build();
        final IncrementalInputSplits.Result result = incrementalInputSplits.inputSplits(metaClient, hadoopConf, false);
        if (result.isEmpty()) {
          // When there is no input splits, just return an empty source.
          LOG.warn("No input splits generate for incremental read, returns empty collection instead");
          return InputFormats.EMPTY_INPUT_FORMAT;
        }
        return mergeOnReadInputFormat(rowType, requiredRowType, tableAvroSchema,
            rowDataType, result.getInputSplits(), false);
      default:
        String errMsg = String.format("Invalid query type : '%s', options ['%s', '%s', '%s'] are supported now", queryType,
            FlinkOptions.QUERY_TYPE_SNAPSHOT, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED, FlinkOptions.QUERY_TYPE_INCREMENTAL);
        throw new HoodieException(errMsg);
    }
  }

  public Schema getTableAvroSchema() {
    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(metaClient);
      return schemaResolver.getTableAvroSchema();
    } catch (Throwable e) {
      // table exists but has no written data
      LOG.warn("Get table avro schema error, use schema from the DDL instead", e);
      return inferSchemaFromDdl();
    }
  }

  private Schema inferSchemaFromDdl() {
    Schema schema = AvroSchemaConverter.convertToSchema(this.tableRowType);
    return HoodieAvroUtils.addMetadataFields(schema, conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED));
  }

  private DataType getProducedDataType() {
    //String[] schemaFieldNames = this.schema.getColumnNames().toArray(new String[0]);
    //DataType[] schemaTypes = this.schema.getColumnDataTypes().toArray(new DataType[0]);

    return DataTypes.ROW(Arrays.stream(this.requiredPos)
            .mapToObj(i -> DataTypes.FIELD(schemaFieldNames[i], schemaFieldTypes[i]))
            .toArray(DataTypes.Field[]::new))
        .bridgedTo(RowData.class);
  }

  private List<MergeOnReadInputSplit> buildFileIndex() {
    Set<String> requiredPartitionPaths = getRequiredPartitionPaths();
    fileIndex.setPartitionPaths(requiredPartitionPaths);
    List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
    if (relPartitionPaths.size() == 0) {
      return Collections.emptyList();
    }
    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    if (fileStatuses.length == 0) {
      throw new HoodieException("No files found for reading in user provided path.");
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        // file-slice after pending compaction-requested instant-time is also considered valid
        metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(), fileStatuses);
    String latestCommit = fsView.getLastInstant().get().getTimestamp();
    final String mergeType = this.conf.getString(FlinkOptions.MERGE_TYPE);
    final AtomicInteger cnt = new AtomicInteger(0);
    // generates one input split for each file group
    return relPartitionPaths.stream()
        .map(relPartitionPath -> fsView.getLatestMergedFileSlicesBeforeOrOn(relPartitionPath, latestCommit)
            .map(fileSlice -> {
              String basePath = fileSlice.getBaseFile().map(BaseFile::getPath).orElse(null);
              Option<List<String>> logPaths = Option.ofNullable(fileSlice.getLogFiles()
                  .sorted(HoodieLogFile.getLogFileComparator())
                  .map(logFile -> logFile.getPath().toString())
                  .collect(Collectors.toList()));
              return new MergeOnReadInputSplit(cnt.getAndAdd(1), basePath, logPaths, latestCommit,
                  metaClient.getBasePath(), maxCompactionMemoryInBytes, mergeType, null, fileSlice.getFileId());
            }).collect(Collectors.toList()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  private MergeOnReadInputFormat mergeOnReadInputFormat(
      RowType rowType,
      RowType requiredRowType,
      Schema tableAvroSchema,
      DataType rowDataType,
      List<MergeOnReadInputSplit> inputSplits,
      boolean emitDelete) {
    final MergeOnReadTableState hoodieTableState = new MergeOnReadTableState(
        rowType,
        requiredRowType,
        tableAvroSchema.toString(),
        AvroSchemaConverter.convertToSchema(requiredRowType).toString(),
        inputSplits,
        conf.getString(FlinkOptions.RECORD_KEY_FIELD).split(","));
    return MergeOnReadInputFormat.builder()
        .config(this.conf)
        .tableState(hoodieTableState)
        // use the explicit fields' data type because the AvroSchemaConverter
        // is not very stable.
        .fieldTypes(rowDataType.getChildren())
        .defaultPartName(conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME))
        .limit(this.limit)
        .emitDelete(emitDelete)
        .build();
  }

  private InputFormat<RowData, ?> baseFileOnlyInputFormat() {
    final FileStatus[] fileStatuses = getReadFiles();
    if (fileStatuses.length == 0) {
      return InputFormats.EMPTY_INPUT_FORMAT;
    }

    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants(), fileStatuses);
    Path[] paths = fsView.getLatestBaseFiles()
        .map(HoodieBaseFile::getFileStatus)
        .map(FileStatus::getPath).toArray(Path[]::new);

    return new CopyOnWriteInputFormat(
        FilePathUtils.toFlinkPaths(paths),
        //this.schema.getColumnNames().toArray(new String[0]),
        schemaFieldNames,
        //this.schema.getColumnDataTypes().toArray(new DataType[0]),
        schemaFieldTypes,
        this.requiredPos,
        this.conf.getString(FlinkOptions.PARTITION_DEFAULT_NAME),
        this.limit == NO_LIMIT_CONSTANT ? Long.MAX_VALUE : this.limit, // ParquetInputFormat always uses the limit value
        getParquetConf(this.conf, this.hadoopConf),
        this.conf.getBoolean(FlinkOptions.UTC_TIMEZONE),
        internalSchemaManager
    );
  }

  public FileStatus[] getReadFiles() {
    Set<String> requiredPartitionPaths = getRequiredPartitionPaths();
    fileIndex.setPartitionPaths(requiredPartitionPaths);
    List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
    if (relPartitionPaths.size() == 0) {
      return new FileStatus[0];
    }
    return fileIndex.getFilesInPartitions();
  }

  private Set<String> getRequiredPartitionPaths() {
    if (this.requiredPartitions == null) {
      // returns null for non partition pruning
      return null;
    }
    return FilePathUtils.toRelativePartitionPaths(this.partitionKeys, this.requiredPartitions,
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING));
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (cache != null) {
      cache.clear();
      cache = null;
    }
  }

  @Override
  public String toString() {
    return "HoodieLookupTable";
  }
}
