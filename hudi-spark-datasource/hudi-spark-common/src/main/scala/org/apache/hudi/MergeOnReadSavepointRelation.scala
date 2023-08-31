/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.util.ConfigUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.action.savepoint.SavepointHelpers
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * @Experimental
 */
case class MergeOnReadSavepointRelation(override val sqlContext: SQLContext,
                                          override val optParams: Map[String, String],
                                          override val metaClient: HoodieTableMetaClient,
                                          private val userSchema: Option[StructType],
                                          private val prunedDataSchema: Option[StructType] = None)
  extends BaseMergeOnReadSnapshotRelation(sqlContext, optParams, metaClient, Seq(), userSchema, prunedDataSchema)
    with HoodieIncrementalRelationTrait {

  override type Relation = MergeOnReadSavepointRelation

  override lazy val tableState: HoodieTableState = {
    val recordMergerImpls = ConfigUtils.split2List(getConfigValue(HoodieWriteConfig.RECORD_MERGER_IMPLS)).asScala.toList
    val recordMergerStrategy = getConfigValue(HoodieWriteConfig.RECORD_MERGER_STRATEGY,
      Option(metaClient.getTableConfig.getRecordMergerStrategy))
    val filterBySavepointEventTime = getConfigValue(DataSourceReadOptions.SAVEPOINT_FILTER_BY_EVENT_TIME).toBoolean

    // Subset of the state of table's configuration as of at the time of the query
    HoodieTableState(
      tablePath = basePath.toString,
      latestCommitTimestamp = queryTimestamp,
      recordKeyField = recordKeyField,
      preCombineFieldOpt = preCombineFieldOpt,
      usesVirtualKeys = !tableConfig.populateMetaFields(),
      recordPayloadClassName = tableConfig.getPayloadClass,
      metadataConfig = fileIndex.metadataConfig,
      recordMergerImpls = recordMergerImpls,
      recordMergerStrategy = recordMergerStrategy,
      filterBySavepointEventTime,
      Option.apply(SavepointHelpers.getBoundaryFromInstant(endTimestamp, metaClient, filterBySavepointEventTime))
    )
  }
  override def updatePrunedDataSchema(prunedSchema: StructType): Relation =
    this.copy(prunedDataSchema = Some(prunedSchema))

  override def imbueConfigs(sqlContext: SQLContext): Unit = {
    super.imbueConfigs(sqlContext)
    sqlContext.sparkSession.sessionState.conf.setConfString("spark.sql.parquet.enableVectorizedReader", "false")
  }

  override protected def timeline: HoodieTimeline = {
    val validActions = Set(HoodieTimeline.COMMIT_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION,
      HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION,
      HoodieTimeline.SAVEPOINT_ACTION)

//    metaClient.getActiveTimeline.getTimelineOfActions(validActions.asJava).findInstantsBefore(endTimestamp)
    metaClient.getActiveTimeline.getTimelineOfActions(validActions.asJava)
  }

  protected override def composeRDD(fileSplits: Seq[HoodieMergeOnReadFileSplit],
                                    tableSchema: HoodieTableSchema,
                                    requiredSchema: HoodieTableSchema,
                                    requestedColumns: Array[String],
                                    filters: Array[Filter]): RDD[InternalRow] = {
    // The only required filters are ones that make sure we're only fetching records that
    // fall into incremental span of the timeline being queried
    val requiredFilters = incrementalSpanRecordFilters
    val optionalFilters = filters
    val readers = createBaseFileReaders(tableSchema, requiredSchema, requestedColumns, requiredFilters, optionalFilters)

    // TODO(HUDI-3639) implement incremental span record filtering w/in RDD to make sure returned iterator is appropriately
    //                 filtered, since file-reader might not be capable to perform filtering
    new HoodieMergeOnReadRDD(
      sqlContext.sparkContext,
      config = jobConf,
      fileReaders = readers,
      tableSchema = tableSchema,
      requiredSchema = requiredSchema,
      tableState = tableState,
      mergeType = mergeType,
      fileSplits = fileSplits)
  }

  override protected def collectFileSplits(partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): List[HoodieMergeOnReadFileSplit] = {
    if (includedCommits.isEmpty) {
      List()
    } else {
      val partitions = SavepointHelpers.getPartitionsFromSavepoint(metaClient, endTimestamp)
      val maxInstantTime = endTimestamp

      val fileStatus = fileIndex.getAllInputFileSlices.entrySet().asScala.flatMap(entry => {
        val partitionPath = entry.getKey.getPath
        if (partitions.contains(partitionPath)) {
          SavepointHelpers.convertFileSliceIntoFileStatusUnchecked(entry.getValue).asScala
        } else {
          new Array[FileStatus](0)
        }
      }).toArray

      val fsView = new HoodieTableFileSystemView(metaClient, timeline, fileStatus)
      val fileSlices = partitions.asScala.flatMap(partition => {
        fsView.getLatestMergedFileSlicesBeforeOrOn(partition, maxInstantTime).iterator().asScala
      }).toSeq

      buildSplits(fileSlices)
    }
  }
}

