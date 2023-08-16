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

package org.apache.hudi.sink.event;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import java.util.Objects;

/**
 * An operator event to mark successful checkpoint batch write.
 */
public class WatermarkEvent implements OperatorEvent {
  private static final long serialVersionUID = 1L;
  private int taskID;
  private Long watermarkTime;

  private String instantTime;

  // default constructor for efficient serialization
  public WatermarkEvent() {
  }

  /**
   * Creates an event.
   */
  private WatermarkEvent(
      int taskID,
      Long watermarkTime,
      String instantTime) {
    this.taskID = taskID;
    this.watermarkTime = watermarkTime;
    this.instantTime = instantTime;
  }

  /**
   * Returns the builder for {@link WatermarkEvent}.
   */
  public static Builder builder() {
    return new Builder();
  }

  public int getTaskID() {
    return taskID;
  }

  public void setTaskID(int taskID) {
    this.taskID = taskID;
  }

  public Long getWatermarkTime() {
    return watermarkTime;
  }

  public void setWatermarkTime(Long watermarkTime) {
    this.watermarkTime = watermarkTime;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public void setInstantTime(String instantTime) {
    this.instantTime = instantTime;
  }

  /**
   * Merges this event with given {@link WatermarkEvent} {@code other}.
   *
   * @param other The event to be merged
   */
  public WatermarkEvent mergeWith(WatermarkEvent other) {
    ValidationUtils.checkArgument(this.taskID == other.taskID);
    this.watermarkTime = other.watermarkTime;
    return this;
  }

  @Override
  public String toString() {
    return "WriteMetadataEvent{"
        + ", taskID=" + taskID
        + ", watermarkTime='" + watermarkTime
        + ", instant='" + instantTime
        + '}';
  }
  // -------------------------------------------------------------------------
  //  Builder
  // -------------------------------------------------------------------------

  /**
   * Builder for {@link WatermarkEvent}.
   */
  public static class Builder {
    private Integer taskID;
    private Long watermarkTime;
    private String instant;

    public WatermarkEvent build() {
      Objects.requireNonNull(taskID);
      Objects.requireNonNull(watermarkTime);
      // instant could be null for now
      return new WatermarkEvent(taskID, watermarkTime, instant);
    }

    public Builder taskID(int taskID) {
      this.taskID = taskID;
      return this;
    }

    public Builder watermarkTime(Long watermarkTime) {
      this.watermarkTime = watermarkTime;
      return this;
    }

    public Builder instant(String currentInstant) {
      this.instant = currentInstant;
      return this;
    }
  }
}
