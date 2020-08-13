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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.parser.mysql.MySQLParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;

import org.junit.jupiter.api.Test;

public class MySQLValidatorTest extends SqlValidatorTest {

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(
        SqlTestFactory.INSTANCE
            .with("parserFactory", MySQLParserImpl.FACTORY));
  }

  /** Tests matching of built-in operator names. */
  @Test void testUnquotedBuiltInFunctionNames() {
    final Sql mysql = sql("?")
        .withUnquotedCasing(Casing.UNCHANGED)
        .withQuoting(Quoting.BACK_TICK)
        .withCaseSensitive(false);

    mysql.sql("select sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    mysql.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    mysql.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    mysql.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    mysql.sql("select current_time from dept").ok();
    mysql.sql("select Current_Time from dept").ok();
    mysql.sql("select CURRENT_TIME from dept").ok();

    // MySQL assumes that a quoted function name is not a built-in.
    //
    // mysql> select `sum`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.sum does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select `SUM`(`day`) from days;
    // ERROR 1630 (42000): FUNCTION foodmart.SUM does not exist. Check the
    //   'Function Name Parsing and Resolution' section in the Reference Manual
    // mysql> select SUM(`day`) from days;
    // +------------+
    // | SUM(`day`) |
    // +------------+
    // |         28 |
    // +------------+
    // 1 row in set (0.00 sec)
    //
    // We do not follow MySQL in this regard. `count` is preserved in
    // lower-case, and is matched case-insensitively because it is a built-in.
    // So, the query succeeds.
    mysql.sql("select `count`(*) from dept").ok();
  }
}
