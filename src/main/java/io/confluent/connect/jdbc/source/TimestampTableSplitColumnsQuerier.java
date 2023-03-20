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
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * A specialized subclass of the {@link TimestampIncrementingTableQuerier} that only advances the
 * to-be-committed timestamp offset for its records after either all rows have been read from the
 * current table query, or all rows with the to-be-committed timestamp have been read successfully
 * (and the next row has a timestamp that is strictly greater than it).
 * This prevents data loss in cases where the table has multiple rows with identical timestamps and
 * the connector is shut down in the middle of reading these rows. However, if the connector is
 * configured with additional query logic (such as a suffix containing a LIMIT clause), data loss
 * may still occur, since the timestamp offset for the last row of each query is unconditionally
 * committed.
 */
public class TimestampTableSplitColumnsQuerier extends TimestampTableQuerier {
  private final ColumnDateTime datetimestampColumns;

  public TimestampTableSplitColumnsQuerier(
          DatabaseDialect dialect,
          QueryMode mode,
          String name,
          String topicPrefix,
          List<String> timestampColumnNames,
          Map<String, Object> offsetMap,
          Long timestampDelay,
          TimeZone timeZone,
          String suffix,
          TimestampGranularity timestampGranularity,
          String tableDatetimestampColumnName
  ) {
    super(
            dialect,
            mode,
            name,
            topicPrefix,
            timestampColumnNames,
            offsetMap,
            timestampDelay,
            timeZone,
            suffix,
            timestampGranularity
    );
    this.datetimestampColumns = new ColumnDateTime(tableId, tableDatetimestampColumnName);
  }
}
