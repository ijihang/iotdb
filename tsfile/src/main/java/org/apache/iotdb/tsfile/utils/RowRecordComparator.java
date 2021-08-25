/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.util.Comparator;

public class RowRecordComparator implements Comparator<Object> {

  private final boolean ascending;

  public RowRecordComparator(boolean ascending) {
    this.ascending = ascending;
  }

  @Override
  public int compare(Object o1, Object o2) {

    RowRecord rowRecord1 = (RowRecord) o1;
    RowRecord rowRecord2 = (RowRecord) o2;

    // If the timestamps are equal, compare devices
    if (ascending) {
      if (rowRecord1.getTimestamp() != rowRecord2.getTimestamp()) {
        return rowRecord1.getTimestamp() - rowRecord2.getTimestamp() > 0 ? 1 : -1;
      } else {
        return rowRecord1
            .getFields()
            .get(0)
            .getBinaryV()
            .compareTo(rowRecord2.getFields().get(0).getBinaryV());
      }
    } else {
      if (rowRecord1.getTimestamp() != rowRecord2.getTimestamp()) {
        return rowRecord2.getTimestamp() - rowRecord1.getTimestamp() > 0 ? 1 : -1;
      } else {
        return rowRecord2
            .getFields()
            .get(0)
            .getBinaryV()
            .compareTo(rowRecord1.getFields().get(0).getBinaryV());
      }
    }
  }
}
