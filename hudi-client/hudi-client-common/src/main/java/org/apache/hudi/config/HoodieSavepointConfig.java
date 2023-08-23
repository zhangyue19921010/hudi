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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Archival related config.
 */
@Immutable
@ConfigClassProperty(name = "Savepoint Configs",
    groupName = ConfigGroups.Names.WRITE_CLIENT,
    description = "Configurations that control savepoint.")
public class HoodieSavepointConfig extends HoodieConfig {

  public static final ConfigProperty<String> AUTO_SAVEPOINT = ConfigProperty
      .key("hoodie.savepoint.automatic")
      .defaultValue("false")
      .withDocumentation("");

  // write related config ------------------------------------------------------------------------------------------------------------
  public static final ConfigProperty<Boolean> SAVEPOINT_RECORD_EVENT_TIME = ConfigProperty
      .key("hoodie.savepoint.write.record.eventtime")
      .defaultValue(true)
      .withDocumentation("");

  public static final ConfigProperty<String> SAVEPOINT_WRITE_TIME_EXPRESSION = ConfigProperty
      .key("hoodie.savepoint.write.time.expression")
      .defaultValue("")
      .withDocumentation("");



  // read related config ------------------------------------------------------------------------------------------------------------
  public static final ConfigProperty<Boolean> SAVEPOINT_FILTER_BY_ENENT_TIME = ConfigProperty
      .key("hoodie.savepoint.read.filterby.eventtime")
      .defaultValue(false)
      .withDocumentation("");

  public static final ConfigProperty<String> SAVE_POINT_READ_DATE = ConfigProperty
      .key("hoodie.savepoint.read.date")
      .noDefaultValue()
      .withDocumentation("");


  private HoodieSavepointConfig() {
    super();
  }

  public static HoodieSavepointConfig.Builder newBuilder() {
    return new HoodieSavepointConfig.Builder();
  }

  public static class Builder {

    private final HoodieSavepointConfig savepointConfig = new HoodieSavepointConfig();

    public HoodieSavepointConfig.Builder fromFile(File propertiesFile) throws IOException {
      try (FileReader reader = new FileReader(propertiesFile)) {
        this.savepointConfig.getProps().load(reader);
        return this;
      }
    }

    public HoodieSavepointConfig.Builder fromProperties(Properties props) {
      this.savepointConfig.getProps().putAll(props);
      return this;
    }

    public HoodieSavepointConfig build() {
      savepointConfig.setDefaults(HoodieSavepointConfig.class.getName());
      return savepointConfig;
    }
  }
}
