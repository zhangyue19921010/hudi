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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.FlatLists;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class RowWithEarliestAvroPayload extends BaseAvroPayload
    implements HoodieRecordPayload<RowWithEarliestAvroPayload> {

  /**
   * payload compare ignore columns.eg:a,b
   */
  public static final String PAYLOAD_IGNORE_COLUMNS = "payload.ignore.columns";

  public RowWithEarliestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public RowWithEarliestAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0);
  }

  @Override
  public RowWithEarliestAvroPayload preCombine(RowWithEarliestAvroPayload oldValue) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return combineAndGetUpdateValue(currentValue, schema, null);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue,
      Schema schema,
      Properties properties) throws IOException {
    Option<IndexedRecord> newValueOption = getInsertValue(schema);
    if (!newValueOption.isPresent() || isDeleteRecord((GenericRecord) newValueOption.get())) {
      return Option.empty();
    } else {
      String ignoreColumns = properties.getProperty(PAYLOAD_IGNORE_COLUMNS);
      Set<String> ignoreColumnSet = new HashSet<>();
      if (ignoreColumns != null) {
        String[] columnArray = ignoreColumns.split(",");
        Collections.addAll(ignoreColumnSet, columnArray);
      }
      List<String> compareFields = schema.getFields()
          .stream()
          .map(Schema.Field::name)
          .filter(name -> !ignoreColumnSet.contains(name))
          .collect(Collectors.toList());

      FlatLists.ComparableList<Comparable> newValList =
          getRecordComparableList((GenericRecord) newValueOption.get(), compareFields);


      FlatLists.ComparableList<Comparable> curValList =
          getRecordComparableList((GenericRecord) currentValue, compareFields);


      if (newValList.compareTo(curValList) != 0) {
        return newValueOption;
      } else {
        return Option.of(HoodieRecord.SENTINEL);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  public FlatLists.ComparableList<Comparable> getRecordComparableList(GenericRecord genericRecord, List<String> columns) {
    List<Object> list = new ArrayList<>();
    for (String col : columns) {
      list.add(HoodieAvroUtils.getNestedFieldVal(genericRecord, col, true, true));
    }
    return FlatLists.ofComparableArray(list.toArray());
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0 || isDeletedRecord) {
      return Option.empty();
    }

    return Option.of((IndexedRecord) HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }

  @Override
  public Comparable<?> getOrderingValue() {
    return this.orderingVal;
  }

  @Override
  public boolean canProduceSentinel() {
    return true;
  }
}
