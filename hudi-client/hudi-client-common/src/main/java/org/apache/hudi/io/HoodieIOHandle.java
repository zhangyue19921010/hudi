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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;

import java.util.Properties;

public abstract class HoodieIOHandle<T, I, K, O> {

  protected final String instantTime;
  protected final HoodieWriteConfig config;
  protected final FileSystem fs;
  protected final HoodieTable<T, I, K, O> hoodieTable;
  // maxEventTime for processed records
  protected long eventTimeMax = 0;
  // minEventTime for processed records
  protected long eventTimeMin = 0;
  protected boolean recordEventTime;

  HoodieIOHandle(HoodieWriteConfig config, Option<String> instantTime, HoodieTable<T, I, K, O> hoodieTable) {
    this.instantTime = instantTime.orElse(StringUtils.EMPTY_STRING);
    this.config = config;
    this.hoodieTable = hoodieTable;
    this.fs = getFileSystem();
    this.recordEventTime = config.needRecordEventTimeInCommit();
  }

  public abstract FileSystem getFileSystem();

  public void resetEventTime() {
    this.eventTimeMin = 0;
    this.eventTimeMax = 0;
  }

  public void putEventTimeStats(HoodieWriteStat stat) {
    if (this.recordEventTime) {
      stat.setMaxEventTime(this.eventTimeMax);
      stat.setMinEventTime(this.eventTimeMin);
    }
  }

  public void updateEventTime(HoodieRecord record, Schema writeSchema, Properties props) {
    if (this.recordEventTime) {
      long eventTime = ((Number) record.getOrderingValue(writeSchema, props)).longValue();
      this.eventTimeMax = Math.max(eventTimeMax, eventTime);
      this.eventTimeMin = Math.max(eventTimeMin, eventTime);
    }
  }
}
