/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.util;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for configuration properties.
 */
public class ColumnDateTime {

  private ColumnId columnDate;
  private ColumnId columnTime;

  private static final Logger log = LoggerFactory.getLogger(
          ColumnDateTime.class
  );


  public ColumnDateTime(TableId tableId, String mapTableColumns) {
    String[] arrOfTableColumns = mapTableColumns.split("|");


    log.warn("++++++++ tableId {}, date {}", tableId, arrOfTableColumns[1]);
    log.warn("++++++++ tableId {}, time {}", tableId, arrOfTableColumns[2]);
    this.columnDate = new ColumnId(tableId, arrOfTableColumns[1]);
    this.columnTime = new ColumnId(tableId, arrOfTableColumns[2]);
  }

  public ColumnId getColumnDate() {
    return columnDate;
  }

  public void setColumnDate(ColumnId columnDate) {
    this.columnDate = columnDate;
  }

  public ColumnId getColumnTime() {
    return columnTime;
  }

  public void setColumnTime(ColumnId columnTime) {
    this.columnTime = columnTime;
  }
}
