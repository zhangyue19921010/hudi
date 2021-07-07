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

package org.apache.hudi.utilities;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jetbrains.annotations.TestOnly;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HoodieClusteringJob {

  private static final Logger LOG = LogManager.getLogger(HoodieClusteringJob.class);
  private final Config cfg;
  private transient FileSystem fs;
  private TypedProperties props;
  private final JavaSparkContext jsc;

  public HoodieClusteringJob(JavaSparkContext jsc, Config cfg) {
    this.cfg = cfg;
    this.jsc = jsc;
    // 初始化hoodie相关的配置文件，如果用户指定Path则以Path为准，否则解析--hoodie-conf key=value
    this.props = cfg.propsFilePath == null
        ? UtilHelpers.buildProperties(cfg.configs)
        : readConfigFromFileSystem(jsc, cfg);
  }

  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, Config cfg) {
    final FileSystem fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());

    return UtilHelpers
        .readConfig(fs, new Path(cfg.propsFilePath), cfg.configs)
        .getConfig();
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--base-path", "-sp"}, description = "Base path for the table", required = true)
    public String basePath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--instant-time", "-it"}, description = "Clustering Instant time, only need when cluster. "
        + "And schedule clustering can generate it.", required = false)
    public String clusteringInstantTime = null;
    @Parameter(names = {"--parallelism", "-pl"}, description = "Parallelism for hoodie insert", required = false)
    public int parallelism = 1;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
    public int retry = 0;

    @Parameter(names = {"--schedule", "-sc"}, description = "Schedule clustering")
    public Boolean runSchedule = false;
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for clustering")
    public String propsFilePath = null;

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
            splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();
  }

  public static void main(String[] args) {
    // 初始化配置文件
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    // --help print details
    if (cfg.help || args.length == 0 || (!cfg.runSchedule && cfg.clusteringInstantTime == null)) {
      cmd.usage();
      System.exit(1);
    }
    final JavaSparkContext jsc = UtilHelpers.buildSparkContext("clustering-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory);

    // 实例化HoodieClusteringJob对象 并init 对应配置文件
    HoodieClusteringJob clusteringJob = new HoodieClusteringJob(jsc, cfg);

    // run clustering job with retry
    int result = clusteringJob.cluster(cfg.retry);
    String resultMsg = String.format("Clustering with basePath: %s, tableName: %s, runSchedule: %s",
        cfg.basePath, cfg.tableName, cfg.runSchedule);
    if (result == -1) {
      LOG.error(resultMsg + " failed");
    } else {
      LOG.info(resultMsg + " success");
    }
    jsc.stop();
  }

  public int cluster(int retry) {
    this.fs = FSUtils.getFs(cfg.basePath, jsc.hadoopConfiguration());
    int ret = UtilHelpers.retry(retry, () -> {
      if (cfg.runSchedule) {
        LOG.info("Do schedule");
        // Schedule -> 生成Cluster Plan并返回对应的instantTime
        Option<String> instantTime = doSchedule(jsc);
        int result = instantTime.isPresent() ? 0 : -1;
        if (result == 0) {
          LOG.info("The schedule instant time is " + instantTime.get());
        }
        return result;
      } else {
        LOG.info("Do cluster");
        return doCluster(jsc);
      }
    }, "Cluster failed");
    return ret;
  }

  // 要求至少有一个state=complete action=commit/deltacommit/replacecommit 的Instant
  private String getSchemaFromLatestInstant() throws Exception {
    // 初始化hoodieTableMetaClient
    // 可以通过hoodieTableMetaClient获取表的元数据信息 --> .hoodie 目录
    // It returns meta-data about commits, savepoints, compactions, cleanups as a HoodieTimeline
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(cfg.basePath).setLoadActiveTimelineOnLoad(true).build();
    TableSchemaResolver schemaUtil = new TableSchemaResolver(metaClient);
    // 从activeTimeline中获取commit deltacommit replacecommit 对应的子timeline 并获取complete状态的Instants
    // 若没有处于complete状态的instants则报错
    if (metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      throw new HoodieException("Cannot run clustering without any completed commits");
    }

    // Gets schema for a hoodie table in Avro format without metadata fields.
    // 因为前面保证了至少有一个可用的instant 以此为base 获取schema
    Schema schema = schemaUtil.getTableAvroSchema(false);
    return schema.toString();
  }

  private int doCluster(JavaSparkContext jsc) throws Exception {
    // 基于最新的Instant => 获取最新的schema -- 可以复用
    String schemaStr = getSchemaFromLatestInstant();

    // 初始化SparkRDDWriteClient为hoodieClient
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {

      // do cluster
      JavaRDD<WriteStatus> writeResponse =
              client.cluster(cfg.clusteringInstantTime, true).getWriteStatuses();
      return UtilHelpers.handleErrors(jsc, cfg.clusteringInstantTime, writeResponse);
    }
  }

  @TestOnly
  public Option<String> doSchedule() throws Exception {
    return this.doSchedule(jsc);
  }

  private Option<String> doSchedule(JavaSparkContext jsc) throws Exception {

    // 基于最新的Instant => 获取最新的schema -- 可以复用
    String schemaStr = getSchemaFromLatestInstant();

    // 初始化SparkRDDWriteClient为hoodieClient
    try (SparkRDDWriteClient<HoodieRecordPayload> client = UtilHelpers.createHoodieClient(jsc, cfg.basePath, schemaStr, cfg.parallelism, Option.empty(), props)) {

      // 可以自己设置InstantTime
      if (cfg.clusteringInstantTime != null) {
        client.scheduleClusteringAtInstant(cfg.clusteringInstantTime, Option.empty());
        return Option.of(cfg.clusteringInstantTime);
      }
      return client.scheduleClustering(Option.empty());
    }
  }
}
