/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.bigquery.BigQueryParserImpl;

import org.junit.jupiter.api.Test;

/**
 * Tests the "BigQuery" SQL parser.
 */
final class BigQueryParserTest extends SqlDialectParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return BigQueryParserImpl.FACTORY;
  }

  @Test public void test() {

  }

  @Test public void testMergeIntoBigQuery() {
    final String sql = "merge into t1 a using t2 b on a.x = b.x when matched then "
        + "update set y = b.y when not matched then insert (x,y) values (b.x, b.y)";
    final String expected = "MERGE INTO t1 AS a\n"
        + "USING t2 AS b\n"
        + "ON (a.x = b.x)\n"
        + "WHEN MATCHED THEN UPDATE SET y = b.y\n"
        + "WHEN NOT MATCHED THEN INSERT (x, y) VALUES (b.x, b.y)";
    sql(sql)
        .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
        .ok(expected);
  }
}
