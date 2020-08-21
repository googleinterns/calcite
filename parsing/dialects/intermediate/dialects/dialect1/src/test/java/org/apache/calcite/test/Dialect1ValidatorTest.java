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

import org.apache.calcite.sql.SqlBeginEndCall;
import org.apache.calcite.sql.SqlConditionalStmt;
import org.apache.calcite.sql.SqlConditionalStmtListPair;
import org.apache.calcite.sql.SqlCreateProcedure;
import org.apache.calcite.sql.SqlDeclareConditionStmt;
import org.apache.calcite.sql.SqlDeclareHandlerStmt;
import org.apache.calcite.sql.SqlIterateStmt;
import org.apache.calcite.sql.SqlIterationStmt;
import org.apache.calcite.sql.SqlLeaveStmt;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSignal;
import org.apache.calcite.sql.SqlWhileStmt;
import org.apache.calcite.sql.parser.dialect1.Dialect1ParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

public class Dialect1ValidatorTest extends SqlValidatorTestCase {

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(
        SqlTestFactory.INSTANCE
            .with("parserFactory", Dialect1ParserImpl.FACTORY)
            .with("conformance", SqlConformanceEnum.LENIENT)
            .with("identifierExpansion", true)
            .with("allowUnknownTables", true));
  }

  public SqlNode parseAndValidate(String sql) {
    SqlValidator validator = getTester().getValidator();
    return getTester().parseAndValidate(validator, sql);
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

  @Test public void testCreateFunctionCompoundIdentifier() {
    String ddl = "create function foo.bar() "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 1";
    String query = "select foo.bar()";
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
    sql(ddl2).fails("Error: a function of this name with the same parameters"
        + " already exists");
  }

  @Test public void testCreateFunctionVarchar() {
    String ddl = "create function foo(x integer) "
        + "returns varchar "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return 'str'";
    String query = "select foo(1)";
    sql(ddl).ok();
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

  @Test public void testCreateProcedureBeginEndLabel() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "leave label1;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) beginEnd.statements.get(0);
    assertThat(beginEnd, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureBeginEndNestedOuterLabel() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "label2: begin\n"
        + "leave label1;\n"
        + "end;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlBeginEndCall nestedBeginEnd
        = (SqlBeginEndCall) beginEnd.statements.get(0);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) nestedBeginEnd.statements.get(0);
    assertThat(beginEnd, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureBeginEndNestedInnerLabel() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "label2: begin\n"
        + "leave label2;\n"
        + "end;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlBeginEndCall nestedBeginEnd
        = (SqlBeginEndCall) beginEnd.statements.get(0);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) nestedBeginEnd.statements.get(0);
    assertThat(nestedBeginEnd, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureBeginEndNestedSameNameLabel() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "label1: begin\n"
        + "leave label1;\n"
        + "end;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlBeginEndCall nestedBeginEnd
        = (SqlBeginEndCall) beginEnd.statements.get(0);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) nestedBeginEnd.statements.get(0);
    assertThat(nestedBeginEnd, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureBeginEndSameLevelLabel() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "label1: begin\n"
        + "select a from abc;\n"
        + "end;\n"
        + "label2: begin\n"
        + "leave label1;\n"
        + "end;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlBeginEndCall nestedBeginEnd
        = (SqlBeginEndCall) beginEnd.statements.get(1);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) nestedBeginEnd.statements.get(0);
    assertThat(beginEnd, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureBeginEndNullLabel() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "leave label1;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) beginEnd.statements.get(0);
    assertThat(leaveStmt.labeledBlock, nullValue());
  }

  @Test public void testCreateProcedureIterationStatementLabel() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "label1: while bar = 1 do\n"
        + "leave label1;\n"
        + "end while label1;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlWhileStmt whileLoop = (SqlWhileStmt) beginEnd.statements.get(0);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) whileLoop.statements.get(0);
    assertThat(whileLoop, sameInstance(leaveStmt.labeledBlock));
  }

  @Test public void testCreateProcedureConditionalStatementLeaveCall() {
    String sql = "create procedure foo()\n"
        + "label1: begin\n"
        + "if a = 3 then\n"
        + "leave label1;\n"
        + "end if;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlConditionalStmt conditionalStmt
        = (SqlConditionalStmt) beginEnd.statements.get(0);
    SqlConditionalStmtListPair listPair
        = (SqlConditionalStmtListPair) conditionalStmt
        .conditionalStmtListPairs.get(0);
    SqlLeaveStmt leaveStmt = (SqlLeaveStmt) listPair.stmtList.get(0);
    assertThat(beginEnd, sameInstance(leaveStmt.labeledBlock));
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

  @Test public void testColumnAtLocal() {
    String sql = "select hiredate at local from emp";
    String expectedType = "RecordType(TIMESTAMP(0) NOT NULL EXPR$0) NOT NULL";
    String expectedRewrite = "SELECT `EMP`.`HIREDATE` AT LOCAL\n"
        + "FROM `CATALOG`.`SALES`.`EMP` AS `EMP`";
    sql(sql).type(expectedType).rewritesTo(expectedRewrite);
  }

  @Test public void testCreateProcedureSelect() {
    String sql = "create procedure foo() select a from abc";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureInsert() {
    String sql = "create procedure foo() insert into empnullables "
        + "(empno, ename) values (1, 'hello')";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "INSERT INTO `CATALOG`.`SALES`.`EMPNULLABLES` (`EMPNO`, `ENAME`)\n"
        + "VALUES ROW(1, 'hello')";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureDelete() {
    String sql = "create procedure foo() delete from emp where deptno = 10";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "DELETE FROM `CATALOG`.`SALES`.`EMP`\n"
        + "WHERE `DEPTNO` = 10";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureMerge() {
    String sql = "create procedure foo() merge into t1 a using t2 b on a.x = "
        + "b.x when matched then update set y = b.y when not matched then "
        + "insert (x,y) values (b.x, b.y)";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "MERGE INTO `CATALOG`.`T1` AS `A`\n"
        + "USING `T2` AS `B`\n"
        + "ON `A`.`X` = `B`.`X`\n"
        + "WHEN MATCHED THEN UPDATE SET `Y` = `B`.`Y`\n"
        + "WHEN NOT MATCHED THEN INSERT (`X`, `Y`) "
        + "(VALUES ROW(`B`.`X`, `B`.`Y`))";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureUpdate() {
    String sql = "create procedure foo() update emp set deptno = 10";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "UPDATE `CATALOG`.`SALES`.`EMP` SET `DEPTNO` = 10";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureSelectNestedBeginEnd() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "select a from abc;\n"
        + "end";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "BEGIN\n"
        + "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`;\n"
        + "END";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureSelectNestedIteration() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "loop\n"
        + "select a from abc;\n"
        + "end loop;\n"
        + "end";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "BEGIN\n"
        + "LOOP "
        + "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`;\n"
        + "END LOOP;\n"
        + "END";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureSelectNestedConditional() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "if a = 2 then\n"
        + "select a from abc;\n"
        + "end if;\n"
        + "end";
    String expected = "CREATE PROCEDURE `FOO` ()\n"
        + "BEGIN\n"
        + "IF `A` = 2 THEN "
        + "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`;\n"
        + "END IF;\n"
        + "END";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCreateProcedureConditionSignal() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "declare bar condition;\n"
        + "signal bar;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlDeclareConditionStmt declareCondition
        = (SqlDeclareConditionStmt) beginEnd.statements.get(0);
    SqlSignal signal = (SqlSignal) beginEnd.statements.get(1);
    assertThat(signal.conditionDeclaration, sameInstance(declareCondition));
  }

  @Test public void testCreateProcedureConditionSignalNull() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "signal bar;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlSignal signal = (SqlSignal) beginEnd.statements.get(0);
    assertThat(signal.conditionDeclaration, nullValue());
  }

  @Test public void testCreateProcedureConditionResignalNull() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "resignal;\n"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlSignal resignal = (SqlSignal) beginEnd.statements.get(0);
    assertThat(resignal.conditionDeclaration, nullValue());
  }

  @Test public void testCreateProcedureConditionHandler() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "declare bar condition;"
        + "declare continue handler for bar select baz;"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlDeclareConditionStmt declareCondition
        = (SqlDeclareConditionStmt) beginEnd.statements.get(0);
    SqlDeclareHandlerStmt handler
        = (SqlDeclareHandlerStmt) beginEnd.statements.get(1);
    assertThat(handler.conditionDeclarations.contains(declareCondition),
        equalTo(true));
  }

  @Test public void testCreateProcedureConditionHandlerNull() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "declare bar condition;"
        + "declare continue handler for baz select qux;"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlDeclareHandlerStmt handler
        = (SqlDeclareHandlerStmt) beginEnd.statements.get(1);
    assertThat(handler.conditionDeclarations.size(), equalTo(0));
  }

  @Test public void testCreateProcedureConditionHandlerMultipleConditions() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "declare bar condition;"
        + "declare baz condition;"
        + "declare qux condition;"
        + "declare continue handler for bar, baz, qux select abc;"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlDeclareHandlerStmt handler
        = (SqlDeclareHandlerStmt) beginEnd.statements.get(3);
    assertThat(handler.conditionDeclarations.size(), equalTo(3));
    for (int i = 0; i < 3; i++) {
      SqlDeclareConditionStmt declareCondition
          = (SqlDeclareConditionStmt) beginEnd.statements.get(i);
      assertThat(handler.conditionDeclarations.contains(declareCondition),
          equalTo(true));
    }
  }

  @Test public void testCreateProcedureConditionHandlerDeclaresCondition() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "declare bar condition for sqlwarning, sqlexception, not found, "
        + "bar select baz;"
        + "signal bar;"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlDeclareHandlerStmt handler
        = (SqlDeclareHandlerStmt) beginEnd.statements.get(0);
    SqlSignal signal = (SqlSignal) beginEnd.statements.get(1);
    assertThat(handler.conditionDeclarations.contains(handler), equalTo(true));
    assertThat(signal.conditionDeclaration, sameInstance(handler));
  }

  @Test public void testCreateProcedureConditionIterate() {
    String sql = "create procedure foo()\n"
        + "begin\n"
        + "label1: while a < 1 do "
        + "iterate label1;\n"
        + "end while label1;"
        + "end";
    SqlCreateProcedure node = (SqlCreateProcedure) parseAndValidate(sql);
    SqlBeginEndCall beginEnd = (SqlBeginEndCall) node.statement;
    SqlIterationStmt whileLoop
        = (SqlIterationStmt) beginEnd.statements.get(0);
    SqlIterateStmt iterate = (SqlIterateStmt) whileLoop.statements.get(0);
    assertThat(iterate.labeledBlock, sameInstance(whileLoop));
  }

  @Test public void testCastWithColumnAttributeFormat() {
    String sql = "select deptno (format 'YYYYMMDD') from dept";
    String expectedType = "RecordType(INTEGER NOT NULL EXPR$0) NOT NULL";
    String expectedRewrite = "SELECT CAST(`DEPT`.`DEPTNO` AS FORMAT "
        + "'YYYYMMDD')\n"
        + "FROM `CATALOG`.`SALES`.`DEPT` AS `DEPT`";
    sql(sql).type(expectedType).rewritesTo(expectedRewrite);
  }
}
