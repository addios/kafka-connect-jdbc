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

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.TimestampGranularity;
import io.confluent.connect.jdbc.util.ColumnDateTime;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * <p>
 *   TimestampIncrementingTableQuerier performs incremental loading of data using two mechanisms: a
 *   timestamp column provides monotonically-incrementing values that can be used to detect new or
 *   modified rows and a strictly-incrementing (e.g. auto increment) column allows detecting new
 *   rows or combined with the timestamp provide a unique identifier for each update to the row.
 * </p>
 * <p>
 *   At least one of the two columns must be specified (or left as "" for the incrementing column
 *   to indicate use of an auto-increment column). If both columns are provided, they are both
 *   used to ensure only new or updated rows are reported and to totally order updates so
 *   recovery can occur no matter when offsets were committed. If only an incrementing field is
 *   provided, new rows will be detected but not updates. If only timestamp fields are
 *   provided, both new and updated rows will be detected, but stream offsets will not be unique
 *   so failures may cause duplicates.
 * </p>
 */
public class TimestampIncrementingTableSplitColumnsQuerier
        extends TimestampIncrementingTableQuerier {
  private static final Logger log = LoggerFactory.getLogger(
      TimestampIncrementingTableSplitColumnsQuerier.class
  );

  private final ColumnDateTime datetimestampColumns;

  public TimestampIncrementingTableSplitColumnsQuerier(DatabaseDialect dialect, QueryMode mode,
                                                      String name, String topicPrefix,
                                                      List<String> timestampColumnNames,
                                                      String incrementingColumnName,
                                                      Map<String, Object> offsetMap,
                                                      Long timestampDelay, TimeZone timeZone,
                                                      String suffix,
                                                      TimestampGranularity timestampGranularity,
                                                      String tableDatetimestampColumnName) {
    super(dialect, mode, name, topicPrefix, timestampColumnNames, incrementingColumnName, offsetMap,
            timestampDelay, timeZone, suffix, timestampGranularity);
    this.datetimestampColumns = new ColumnDateTime(tableId, tableDatetimestampColumnName);
  }


  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    findDefaultAutoIncrementingColumn(db);

    ColumnId incrementingColumn = null;
    if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
      incrementingColumn = new ColumnId(tableId, incrementingColumnName);
    }

    ExpressionBuilder builder = dialect.expressionBuilder();
    switch (mode) {
      case TABLE:
        builder.append("SELECT * FROM ");
        builder.append(tableId);
        break;
      case QUERY:
        builder.append(query);
        break;
      default:
        throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
    }

    log.debug("Table {} , dateColumns {}, timeColumns {}",
            incrementingColumn,
            datetimestampColumns.getColumnDate().name(),
            datetimestampColumns.getColumnTime().name());
    criteria = dialect.criteriaFor(incrementingColumn, datetimestampColumns, "");
    // Append the criteria using the columns ...

    criteria.whereClause(builder);

    addSuffixIfPresent(builder);

    String queryString = builder.toString();
    recordQuery(queryString);
    log.trace("{} prepared SQL query: {}", this, queryString);
    stmt = dialect.createPreparedStatement(db, queryString);
  }

}