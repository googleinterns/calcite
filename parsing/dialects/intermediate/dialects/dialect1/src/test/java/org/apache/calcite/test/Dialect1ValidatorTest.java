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

import org.apache.calcite.sql.parser.dialect1.Dialect1ParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import org.junit.jupiter.api.Test;

public class Dialect1ValidatorTest extends SqlValidatorTestCase {

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(
        SqlTestFactory.INSTANCE
            .with("parserFactory", Dialect1ParserImpl.FACTORY)
            .with("conformance", SqlConformanceEnum.LENIENT)
            .with("identifierExpansion", true)
            .with("allowUnknownTables", true));
  }

  @Test public void testSel() {
    String sql = "sel a from abc";
    String expected = "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testFirstValue() {
    String sql = "SELECT FIRST_VALUE (foo) OVER (PARTITION BY (foo)) FROM bar";
    String expected = "SELECT FIRST_VALUE(`BAR`.`FOO`) OVER (PARTITION BY "
        + "`BAR`.`FOO`)\n"
        + "FROM `BAR` AS `BAR`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testLastValue() {
    String sql = "SELECT LAST_VALUE (foo) OVER (PARTITION BY (foo)) FROM bar";
    String expected = "SELECT LAST_VALUE(`BAR`.`FOO`) OVER (PARTITION BY "
        + "`BAR`.`FOO`)\n"
        + "FROM `BAR` AS `BAR`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testFirstValueIgnoreNulls() {
    final String sql = "SELECT FIRST_VALUE (foo IGNORE NULLS) OVER"
        + " (PARTITION BY (foo)) FROM bar";
    final String expected = "SELECT FIRST_VALUE(`BAR`.`FOO` IGNORE NULLS)"
        + " OVER (PARTITION BY `BAR`.`FOO`)\n"
        + "FROM `BAR` AS `BAR`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testFirstValueRespectNulls() {
    final String sql = "SELECT FIRST_VALUE (foo RESPECT NULLS) OVER"
        + " (PARTITION BY (foo)) FROM bar";
    final String expected = "SELECT FIRST_VALUE(`BAR`.`FOO` RESPECT NULLS)"
        + " OVER (PARTITION BY `BAR`.`FOO`)\n"
        + "FROM `BAR` AS `BAR`";
    sql(sql).rewritesTo(expected);
  }

  // The sql() call removes "^" symbols in the query, so this test calls
  // checkRewrite() which does not remove the caret operator.
  @Test public void testCaretNegation() {
    String sql = "select a from abc where ^a = 1";
    String expected = "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`\n"
        + "WHERE ^`ABC`.`A` = 1";
    getTester().checkRewrite(sql, expected);
  }

  @Test public void testHostVariable() {
    String sql = "select :a from abc";
    String expected = "SELECT :A\n"
        + "FROM `ABC` AS `ABC`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testInlineModOperatorWithExpressions() {
    String sql = "select (select a from abc) mod (select d from def) from ghi";
    String expected = "SELECT MOD(((SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`)), ((SELECT `DEF`.`D`\n"
        + "FROM `DEF` AS `DEF`)))\n"
        + "FROM `GHI` AS `GHI`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateFunction() {
    String ddl = "create function foo() "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select foo()";
    sql(ddl).ok();
    sql(query).type("RecordType(INTEGER NOT NULL EXPR$0) NOT NULL");
  }

  @Test public void testCreateFunctionWithParams() {
    String ddl = "create function foo(x integer, y varchar) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select foo(1, 'str')";
    sql(ddl).ok();
    sql(query).type("RecordType(INTEGER NOT NULL EXPR$0) NOT NULL");
  }

  @Test public void testCreateFunctionOverwrite() {
    String ddl = "create function foo(x integer) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String ddl2 = "create function foo(x integer) "
        + "returns varchar "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 'str'";
    String query = "select foo(1)";
    sql(ddl).ok();
    sql(query).type("RecordType(INTEGER NOT NULL EXPR$0) NOT NULL");
    sql(ddl2).ok();
    sql(query).type("RecordType(VARCHAR NOT NULL EXPR$0) NOT NULL");
  }

  @Test public void testCreateFunctionWrongTypeGetsCasted() {
    String ddl = "create function foo(x varchar) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select foo(1)";
    sql(ddl).ok();
    sql(query).rewritesTo("SELECT `FOO`(CAST(1 AS VARCHAR CHARACTER SET"
        + " `ISO-8859-1`))");
  }

  @Test public void testCreateFunctionOverloaded() {
    String ddl = "create function foo(x integer, y varchar) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String ddl2 = "create function foo(x integer) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select foo(1)";
    String query2 = "select foo(1, 'str')";
    sql(ddl).ok();
    sql(ddl2).ok();
    sql(query).ok();
    sql(query2).ok();
  }

  @Test public void testCreateFunctionWrongNumberOfParametersFails() {
    String ddl = "create function foo(x integer, y varchar) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select ^foo(1)^";
    sql(ddl).ok();
    sql(query).fails("No match found for function signature FOO\\(<NUMERIC>\\)");
  }

  @Test public void testCreateFunctionNonExistentFunctionFails() {
    String ddl = "create function foo(x integer) "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select ^bar(1)^";
    sql(ddl).ok();
    sql(query).fails("No match found for function signature BAR\\(<NUMERIC>\\)");
  }

  @Test public void testCreateTableNoColumns() {
    String ddl = "create table foo";
    String query = "select * from foo";
    sql(ddl).ok();
    sql(query).type("RecordType() NOT NULL");
  }

  @Test public void testCreateTableSelectInteger() {
    String ddl = "create table foo(x int, y varchar)";
    String query = "select x from foo";
    sql(ddl).ok();
    sql(query).type("RecordType(INTEGER NOT NULL X) NOT NULL");
  }

  @Test public void testCreateTableSelectVarchar() {
    String ddl = "create table foo(x int, y varchar)";
    String query = "select y from foo";
    sql(ddl).ok();
    sql(query).type("RecordType(VARCHAR NOT NULL Y) NOT NULL");
  }

  @Test public void testCreateTableInsert() {
    String ddl = "create table foo(x int, y varchar)";
    String query = "insert into foo values (1, 'str')";
    sql(ddl).ok();
    sql(query).type("RecordType(INTEGER NOT NULL X, VARCHAR NOT NULL Y)"
        + " NOT NULL");
  }

  @Test public void testCreateTableSelectNonExistentColumnFails() {
    String ddl = "create table foo(x int, y varchar)";
    String query = "select ^z^ from foo";
    sql(ddl).ok();
    sql(query).fails("Column 'Z' not found in any table");
  }

  @Test public void testCreateTableInsertTooManyValuesFails() {
    String ddl = "create table foo(x int, y varchar)";
    String query = "insert into foo values (1, 'str', 1)";
    sql(ddl).ok();
    sql(query).fails("end index \\(3\\) must not be greater than size \\(2\\)");
  }
}
