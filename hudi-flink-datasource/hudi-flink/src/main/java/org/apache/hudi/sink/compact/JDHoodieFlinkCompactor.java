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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink hudi compaction program that can be executed manually.
 */
public class JDHoodieFlinkCompactor extends HoodieFlinkCompactor {

  protected static final Logger LOG = LoggerFactory.getLogger(JDHoodieFlinkCompactor.class);

  public JDHoodieFlinkCompactor(AsyncCompactionService service) {
    super(service);
  }

  public static void main(String[] args) throws Exception {
    FlinkCompactionConfig cfg = getFlinkCompactionConfig(args);
    Configuration conf = FlinkCompactionConfig.toFlinkConfig(cfg);

    JDAsyncCompactionService service = new JDAsyncCompactionService(cfg, conf);

    new JDHoodieFlinkCompactor(service).start(cfg.serviceMode);
  }

  /**
   * Schedules compaction in service.
   */
  public static class JDAsyncCompactionService extends AsyncCompactionService {
    public JDAsyncCompactionService(FlinkCompactionConfig cfg, Configuration conf) throws Exception {
      super(cfg, conf);
    }

    @Override
    protected void compact() throws Exception {
      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.addSource(new JDCompactionPlanSourceFunction(table, writeClient, cfg))
          .name("JD_compaction_source")
          .uid("JD_uid_compaction_source")
          .rebalance()
          .transform("JD_compact_task",
              TypeInformation.of(CompactionCommitEvent.class),
              new CompactOperator(conf))
          .setParallelism(conf.getInteger(FlinkOptions.COMPACTION_TASKS, 1))
          .addSink(new JDCompactionCommitSink(conf))
          .name("JD_compaction_commit")
          .uid("JD_uid_compaction_commit")
          .setParallelism(1);

      env.execute("JD_flink_hudi_compaction");
    }
  }
}
