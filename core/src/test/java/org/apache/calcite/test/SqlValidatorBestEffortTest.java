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

import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;

import org.junit.jupiter.api.Test;

public class SqlValidatorBestEffortTest extends SqlValidatorTestCase {

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(
        SqlTestFactory.INSTANCE
            .with("allowUnknownTables", true)
            .with("lenientOperatorLookup", true));
  }

  @Test public void testSelectColumnFromUnknownTable() {
    String sql = "SELECT bar FROM foo";
    String expected = "RecordType(ANY BAR) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectColumnFromUnknownTableWithWhereClause() {
    String sql = "SELECT bar FROM foo WHERE foo.baz = 5";
    String expected = "RecordType(ANY BAR) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectStarFromUnknownTable() {
    String sql = "SELECT * FROM foo";
    String expected = "RecordType(DYNAMIC_STAR **) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectFromUnknownTableWithJoin() {
    String sql = "SELECT ename FROM emp INNER JOIN foo ON emp.empno = foo.bar";
    String expected = "RecordType(VARCHAR(20) NOT NULL ENAME) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectStarFromUnknownTableWithJoin() {
    String sql = "SELECT * FROM dept INNER JOIN foo ON dept.deptno = foo.bar";
    String expected = "RecordType(INTEGER NOT NULL DEPTNO, VARCHAR(10)"
        + " NOT NULL NAME, DYNAMIC_STAR **) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectUnknownColumnFromUnknownTableAndKnownTable() {
    String sql = "SELECT ename, bar FROM"
        + " emp INNER JOIN foo ON emp.empno = foo.bar";
    String expected = "RecordType(VARCHAR(20) NOT NULL ENAME, ANY BAR)"
        + " NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectUnknownColumnsFromUnknownTables() {
    String sql = "SELECT a, b FROM foo INNER JOIN bar ON foo.x = bar.y";
    String expected = "RecordType(ANY A, ANY B) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectStarFromUnknownTables() {
    String sql = "SELECT * FROM foo INNER JOIN bar ON foo.x = bar.y";
    String expected = "RecordType(DYNAMIC_STAR **, DYNAMIC_STAR **0) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableInWhereSubquery() {
    String sql = "SELECT ename FROM emp WHERE empno IN (SELECT * FROM foo)";
    String expected = "RecordType(VARCHAR(20) NOT NULL ENAME) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableMerge() {
    String sql = "MERGE INTO foo AS f USING dept AS b ON f.bar = b.name"
        + " WHEN MATCHED THEN UPDATE SET baz = b.deptno";
    String expected = "(DynamicRecordRow[]) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableMergeUsingUnknownTable() {
    String sql = "MERGE INTO bar AS b USING foo AS f ON f.x = b.y"
        + " WHEN MATCHED THEN UPDATE SET u = v";
    String expected = "(DynamicRecordRow[]) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableUpdate() {
    String sql = "UPDATE foo SET bar = 5 WHERE baz = 7";
    String expected = "(DynamicRecordRow[BAR]) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableInsert() {
    String sql = "INSERT INTO foo (a, b, c) VALUES (1, 2, 3)";
    String expected = "RecordType(ANY A, ANY B, ANY C) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testUnknownTableDelete() {
    String sql = "DELETE FROM foo WHERE bar = 5";
    String expected = "(DynamicRecordRow[]) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectKnownFunctionWithUnknownColumn() {
    String sql = "SELECT COUNT(bar) as c FROM foo";
    String expected = "RecordType(BIGINT NOT NULL C) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectUnknownFunctionWithUnknownColumn() {
    String sql = "SELECT COUNTZZZ(bar) as c FROM foo";
    String expected = "RecordType(ANY C) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectKnownAndUnknownOperands() {
    String sql = "SELECT CURRENT_DATE + bar as c FROM foo";
    String expected = "RecordType(ANY C) NOT NULL";
    sql(sql).type(expected);
  }

  @Test public void testSelectUnknownColumnFromKnownTableFails() {
    String sql = "SELECT ^foo^ FROM dept";
    String expected = "Column 'FOO' not found in any table";
    sql(sql).fails(expected);
  }

  @Test public void testAmbiguousUnknownColumnNotExpanded() {
    String sql = "SELECT a "
        + "FROM foo "
        + "INNER JOIN bar ON foo.x = bar.x "
        + "INNER JOIN emp on emp.empno = foo.x";
    String expected = "SELECT `A`\n"
        + "FROM `FOO` AS `FOO`\n"
        + "INNER JOIN `BAR` AS `BAR` ON `FOO`.`X` = `BAR`.`X`\n"
        + "INNER JOIN `CATALOG`.`SALES`.`EMP` AS `EMP`"
        + " ON `EMP`.`EMPNO` = `FOO`.`X`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testColumnFromKnownTableExpanded() {
    String sql = "SELECT empno "
        + "FROM foo "
        + "INNER JOIN emp on emp.empno = foo.x";
    String expected = "SELECT `EMP`.`EMPNO`\n"
        + "FROM `FOO` AS `FOO`\n"
        + "INNER JOIN `CATALOG`.`SALES`.`EMP` AS `EMP`"
        + " ON `EMP`.`EMPNO` = `FOO`.`X`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testUnambiguousUnknownColumnExpanded() {
    String sql = "SELECT foo FROM bar";
    String expected = "SELECT `BAR`.`FOO`\n"
        + "FROM `BAR` AS `BAR`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testAmbiguousColumnFromKnownTablesFails() {
    String sql = "SELECT a, ^deptno^ "
        + "FROM foo "
        + "INNER JOIN bar ON foo.x = bar.x "
        + "INNER JOIN emp on emp.empno = foo.x"
        + "INNER JOIN dept on dept.deptno = emp.deptno";
    String expected = "Column 'DEPTNO' is ambiguous";
    sql(sql).fails(expected);
  }

  @Test public void testStarExpansionFromUnknownTable() {
    String sql = "SELECT * FROM foo";
    String expected = "SELECT *\n"
        + "FROM `FOO` AS `FOO`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testStarExpansionFromUnknownTables() {
    String sql = "SELECT * FROM foo INNER JOIN bar ON foo.x = bar.x";
    String expected = "SELECT *\n"
        + "FROM `FOO` AS `FOO`\n"
        + "INNER JOIN `BAR` AS `BAR` ON `FOO`.`X` = `BAR`.`X`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testStarExpansionFromKnownTable() {
    String sql = "SELECT * FROM dept";
    String expected = "SELECT `DEPT`.`DEPTNO`, `DEPT`.`NAME`\n"
        + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testStarExpansionFromKnownAndUnknownTable() {
    String sql = "SELECT * FROM foo INNER JOIN dept ON dept.deptno = foo.x";
    String expected = "SELECT *\n"
        + "FROM `FOO` AS `FOO`\n"
        + "INNER JOIN `CATALOG`.`SALES`.`DEPT` AS `DEPT`"
        + " ON `DEPT`.`DEPTNO` = `FOO`.`X`";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testUnknownTableInsertIntoUnspecifiedColumns() {
    String sql = "INSERT INTO foo VALUES(1, 'a', CURRENT_DATE)";
    String expected = "INSERT INTO `FOO`\n"
        + "VALUES ROW(1, 'a', CURRENT_DATE)";
    sql(sql)
        .withValidatorIdentifierExpansion(true)
        .rewritesTo(expected);
  }

  @Test public void testUnknownTableInsertMismatch() {
    String sql = "INSERT INTO ^foo^ (a, b, c) VALUES (1, 2, 3, 4)";
    String expected = "Number of INSERT target columns \\(3\\) does not equal number of source "
        + "items \\(4\\)";
    sql(sql).fails(expected);
  }
}
