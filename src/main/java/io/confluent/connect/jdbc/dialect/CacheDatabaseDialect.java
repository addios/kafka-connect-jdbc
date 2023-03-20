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

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria;
import io.confluent.connect.jdbc.util.ColumnDateTime;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.IdentifierRules;
import org.apache.kafka.common.config.AbstractConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link DatabaseDialect} for MySQL.
 */
public class CacheDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link CacheDatabaseDialect}.
   */
  private final Logger log = LoggerFactory.getLogger(CacheDatabaseDialect.class);



  public static class Provider extends SubprotocolBasedProvider {
    private final Logger log = LoggerFactory.getLogger(CacheDatabaseDialect.class);

    public Provider() {

      super(CacheDatabaseDialect.class.getSimpleName(), "cache");
      log.warn("++++++++++++++++++++ load cache ");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new CacheDatabaseDialect(config);
    }
  }

  public CacheDatabaseDialect(AbstractConfig config) {
    super(config);

  }

  protected CacheDatabaseDialect(AbstractConfig config, IdentifierRules defaultIdentifierRules) {
    super(config, defaultIdentifierRules);

  }


  public TimestampIncrementingCriteria criteriaFor(
          ColumnId incrementingColumn,
          ColumnDateTime dateTimeColumn
  ) {
    log.debug("CacheDatabaseDialect Table {} , detaTimestampColumns {}",
            incrementingColumn,  dateTimeColumn);
    return new TimestampIncrementingCriteria(incrementingColumn, dateTimeColumn, timeZone);
  }


}
