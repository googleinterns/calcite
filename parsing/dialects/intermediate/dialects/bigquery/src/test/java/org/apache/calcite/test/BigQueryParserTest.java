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

  @Test public void testExceptSingle() {
    final String sql = "SELECT * EXCEPT(a) FROM foo";
    final String expected = "SELECT * EXCEPT (`A`)\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testExceptMultiple() {
    final String sql = "SELECT * EXCEPT(a, b) FROM foo";
    final String expected = "SELECT * EXCEPT (`A`, `B`)\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testExceptCompound() {
    final String sql = "SELECT * EXCEPT(f.a) FROM foo as f";
    final String expected = "SELECT * EXCEPT (`F`.`A`)\n"
        + "FROM `FOO` AS `F`";
    sql(sql).ok(expected);
  }

  @Test public void testExceptMultipleSelectList() {
    final String sql = "SELECT a, * EXCEPT(b), c FROM abc";
    final String expected = "SELECT `A`, * EXCEPT (`B`), `C`\n"
        + "FROM `ABC`";
    sql(sql).ok(expected);
  }

  @Test public void testExceptSelectNotStarFails() {
    final String sql = "SELECT foo EXCEPT(^x^) FROM bar";
    final String expected = "Non-query expression encountered in illegal context";
    sql(sql).fails(expected);
  }
}
