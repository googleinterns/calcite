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
import org.apache.calcite.sql.parser.defaultdialect.DefaultDialectParserImpl;

import org.junit.jupiter.api.Test;

/**
 * Tests the "Default" SQL parser.
 */
final class DefaultDialectParserTest extends SqlDialectParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return DefaultDialectParserImpl.FACTORY;
  }

  @Test
  void testParensInFrom() {
    // UNNEST may not occur within parentheses.
    // FIXME should fail at "unnest"
    sql("select *from ^(^unnest(x))")
        .fails("(?s)Encountered \"\\( unnest\" at .*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^)")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^ as x)")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // <table-name> may not occur within parentheses.
    sql("select * from (^emp^) as x")
        .fails("(?s)Non-query expression encountered in illegal context.*");

    // Parentheses around JOINs are OK, and sometimes necessary.
    if (false) {
      // todo:
      sql("select * from (emp join dept using (deptno))").ok("xx");

      sql("select * from (emp join dept using (deptno)) join foo using (x)").ok("xx");
    }
  }
}
