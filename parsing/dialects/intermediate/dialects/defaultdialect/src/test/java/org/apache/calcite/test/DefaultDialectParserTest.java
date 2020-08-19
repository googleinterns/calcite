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
        .fails("Lexical error at line 1, column 10\\.  Encountered: \"#\" "
            + "\\(35\\), after : \"\"");
  }

  @Test void testLateral() {
    // Bad: LATERAL table
    sql("select * from lateral ^emp^")
        .fails("(?s)Encountered \"emp\" at .*");
    sql("select * from lateral table ^emp^ as e")
        .fails("(?s)Encountered \"emp\" at .*");

    // Bad: LATERAL TABLE schema.table
    sql("select * from lateral table ^scott^.emp")
        .fails("(?s)Encountered \"scott\" at .*");
    final String expected = "SELECT *\n"
        + "FROM LATERAL TABLE(`RAMP`(1))";

    // Good: LATERAL TABLE function(arg, arg)
    sql("select * from lateral table(ramp(1))")
        .ok(expected);
    sql("select * from lateral table(ramp(1)) as t")
        .ok(expected + " AS `T`");
    sql("select * from lateral table(ramp(1)) as t(x)")
        .ok(expected + " AS `T` (`X`)");
    // Bad: Parentheses make it look like a sub-query
    sql("select * from lateral (table^(^ramp(1)))")
        .fails("(?s)Encountered \"\\(\" at .*");

    // Good: LATERAL (subQuery)
    final String expected2 = "SELECT *\n"
        + "FROM LATERAL (SELECT *\n"
        + "FROM `EMP`)";
    sql("select * from lateral (select * from emp)")
        .ok(expected2);
    sql("select * from lateral (select * from emp) as t")
        .ok(expected2 + " AS `T`");
    sql("select * from lateral (select * from emp) as t(x)")
        .ok(expected2 + " AS `T` (`X`)");
  }

  @Test void testParensInFrom() {
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

      sql("select * from (emp join dept using (deptno)) join foo using (x)")
          .ok("xx");
    }
  }

  @Test void testCollectionTableWithLateral2() {
    final String sql = "select * from dept as d\n"
        + "cross join lateral table(ramp(dept.deptno)) as r";
    final String expected = "SELECT *\n"
        + "FROM `DEPT` AS `D`\n"
        + "CROSS JOIN LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)) AS `R`";
    sql(sql).ok(expected);
  }

  @Test void testCollectionTableWithLateral3() {
    // LATERAL before first table in FROM clause doesn't achieve anything, but
    // it's valid.
    final String sql = "select * from lateral table(ramp(dept.deptno)), dept";
    final String expected = "SELECT *\n"
        + "FROM LATERAL TABLE(`RAMP`(`DEPT`.`DEPTNO`)),\n"
        + "`DEPT`";
    sql(sql).ok(expected);
  }

  @Test void testTableSample() {
    final String sql0 = "select * from ("
        + "  select * "
        + "  from emp "
        + "  join dept on emp.deptno = dept.deptno"
        + "  where gender = 'F'"
        + "  order by sal) tablesample substitute('medium')";
    final String expected0 = "SELECT *\n"
        + "FROM (SELECT *\n"
        + "FROM `EMP`\n"
        + "INNER JOIN `DEPT` ON (`EMP`.`DEPTNO` = `DEPT`.`DEPTNO`)\n"
        + "WHERE (`GENDER` = 'F')\n"
        + "ORDER BY `SAL`) TABLESAMPLE SUBSTITUTE('MEDIUM')";
    sql(sql0).ok(expected0);

    final String sql1 = "select * "
        + "from emp as x tablesample substitute('medium') "
        + "join dept tablesample substitute('lar' /* split */ 'ge') on x.deptno"
        + " = dept.deptno";
    final String expected1 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE SUBSTITUTE('MEDIUM')\n"
        + "INNER JOIN `DEPT` TABLESAMPLE SUBSTITUTE('LARGE') ON (`X`.`DEPTNO` "
        + "= `DEPT`.`DEPTNO`)";
    sql(sql1).ok(expected1);

    final String sql2 = "select * "
        + "from emp as x tablesample bernoulli(50)";
    final String expected2 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0)";
    sql(sql2).ok(expected2);

    final String sql3 = "select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(10) ";
    final String expected3 = "SELECT *\n"
        + "FROM `EMP` AS `X` TABLESAMPLE BERNOULLI(50.0) REPEATABLE(10)";
    sql(sql3).ok(expected3);

    // test repeatable with invalid int literal.
    sql("select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(^100000000000000000000^) ")
        .fails("Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");

    // test repeatable with invalid negative int literal.
    sql("select * "
        + "from emp as x "
        + "tablesample bernoulli(50) REPEATABLE(-^100000000000000000000^) ")
        .fails("Literal '100000000000000000000' "
            + "can not be parsed to type 'java\\.lang\\.Integer'");
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
}
