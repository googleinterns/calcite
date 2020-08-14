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

  @Test void testWithTimeZoneFails() {
    expr("cast(x as time with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as time(0) with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as timestamp with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
    expr("cast(x as timestamp(0) with ^time^ zone)")
        .fails("(?s).*Encountered \"time\" at .*");
  }

  @Test void testHavingBeforeGroupFails() {
    final String sql = "select deptno from emp\n"
        + "having count(*) > 5 and deptno < 4 ^group^ by deptno, emp";
    sql(sql).fails("(?s).*Encountered \"group\" at .*");
  }

  @Test void testInvalidToken() {
    // Causes problems to the test infrastructure because the token mgr
    // throws a java.lang.Error. The usual case is that the parser throws
    // an exception.
    sql("values (a^#^b)")
        .fails("Lexical error at line 1, column 10\\.  Encountered: \"#\" \\(35\\), after : \"\"");
  }

  @Test void testNullTreatment() {
    sql("select lead(x) respect nulls over (w) from t")
        .ok("SELECT (LEAD(`X`) RESPECT NULLS OVER (`W`))\n"
            + "FROM `T`");
    sql("select deptno, sum(sal) respect nulls from emp group by deptno")
        .ok("SELECT `DEPTNO`, SUM(`SAL`) RESPECT NULLS\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`");
    sql("select deptno, sum(sal) ignore nulls from emp group by deptno")
        .ok("SELECT `DEPTNO`, SUM(`SAL`) IGNORE NULLS\n"
            + "FROM `EMP`\n"
            + "GROUP BY `DEPTNO`");
    final String sql = "select col1,\n"
        + " collect(col2) ignore nulls\n"
        + "   within group (order by col3)\n"
        + "   filter (where 1 = 0)\n"
        + "   over (rows 10 preceding)\n"
        + " as c\n"
        + "from t\n"
        + "order by col1 limit 10";
    final String expected = "SELECT `COL1`, (COLLECT(`COL2`) IGNORE NULLS"
        + " WITHIN GROUP (ORDER BY `COL3`)"
        + " FILTER (WHERE (1 = 0)) OVER (ROWS 10 PRECEDING)) AS `C`\n"
        + "FROM `T`\n"
        + "ORDER BY `COL1`\n"
        + "FETCH NEXT 10 ROWS ONLY";
    sql(sql).ok(expected);

    // See [CALCITE-2993] ParseException may be thrown for legal
    // SQL queries due to incorrect "LOOKAHEAD(1)" hints
    sql("select lead(x) ignore from t")
        .ok("SELECT LEAD(`X`) AS `IGNORE`\n"
            + "FROM `T`");
    sql("select lead(x) respect from t")
        .ok("SELECT LEAD(`X`) AS `RESPECT`\n"
            + "FROM `T`");
  }

  @Test void testAs() {
    // AS is optional for column aliases
    sql("select x y from t")
        .ok("SELECT `X` AS `Y`\n"
            + "FROM `T`");

    sql("select x AS y from t")
        .ok("SELECT `X` AS `Y`\n"
            + "FROM `T`");
    sql("select sum(x) y from t group by z")
        .ok("SELECT SUM(`X`) AS `Y`\n"
            + "FROM `T`\n"
            + "GROUP BY `Z`");

    // Even after OVER
    sql("select count(z) over w foo from Bids window w as (order by x)")
        .ok("SELECT (COUNT(`Z`) OVER `W`) AS `FOO`\n"
            + "FROM `BIDS`\n"
            + "WINDOW `W` AS (ORDER BY `X`)");

    // AS is optional for table correlation names
    final String expected = "SELECT `X`\n"
        + "FROM `T` AS `T1`";
    sql("select x from t as t1").ok(expected);
    sql("select x from t t1").ok(expected);

    // AS is required in WINDOW declaration
    sql("select sum(x) over w from bids window w ^(order by x)")
        .fails("(?s).*Encountered \"\\(\".*");

    // Error if OVER and AS are in wrong order
    sql("select count(*) as foo ^over^ w from Bids window w (order by x)")
        .fails("(?s).*Encountered \"over\".*");
  }

}
