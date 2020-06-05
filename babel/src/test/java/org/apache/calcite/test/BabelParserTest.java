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

import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.parser.SqlParserTest;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;

import com.google.common.base.Throwables;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the "Babel" SQL parser, that understands all dialects of SQL.
 */
class BabelParserTest extends SqlParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return SqlBabelParserImpl.FACTORY;
  }

  @Test void testReservedWords() {
    assertThat(isReserved("escape"), is(false));
  }

  /** {@inheritDoc}
   *
   * <p>Copy-pasted from base method, but with some key differences.
   */
  @Override @Test protected void testMetadata() {
    SqlAbstractParserImpl.Metadata metadata = getSqlParser("").getMetadata();
    assertThat(metadata.isReservedFunctionName("ABS"), is(true));
    assertThat(metadata.isReservedFunctionName("FOO"), is(false));

    assertThat(metadata.isContextVariableName("CURRENT_USER"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isContextVariableName("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isContextVariableName("ABS"), is(false));
    assertThat(metadata.isContextVariableName("FOO"), is(false));

    assertThat(metadata.isNonReservedKeyword("A"), is(true));
    assertThat(metadata.isNonReservedKeyword("KEY"), is(true));
    assertThat(metadata.isNonReservedKeyword("SELECT"), is(false));
    assertThat(metadata.isNonReservedKeyword("FOO"), is(false));
    assertThat(metadata.isNonReservedKeyword("ABS"), is(true)); // was false

    assertThat(metadata.isKeyword("ABS"), is(true));
    assertThat(metadata.isKeyword("CURRENT_USER"), is(true));
    assertThat(metadata.isKeyword("CURRENT_CATALOG"), is(true));
    assertThat(metadata.isKeyword("CURRENT_SCHEMA"), is(true));
    assertThat(metadata.isKeyword("KEY"), is(true));
    assertThat(metadata.isKeyword("SELECT"), is(true));
    assertThat(metadata.isKeyword("HAVING"), is(true));
    assertThat(metadata.isKeyword("A"), is(true));
    assertThat(metadata.isKeyword("BAR"), is(false));

    assertThat(metadata.isReservedWord("SELECT"), is(true));
    assertThat(metadata.isReservedWord("CURRENT_CATALOG"), is(false)); // was true
    assertThat(metadata.isReservedWord("CURRENT_SCHEMA"), is(false)); // was true
    assertThat(metadata.isReservedWord("KEY"), is(false));

    String jdbcKeywords = metadata.getJdbcKeywords();
    assertThat(jdbcKeywords.contains(",COLLECT,"), is(false)); // was true
    assertThat(!jdbcKeywords.contains(",SELECT,"), is(true));
  }

  @Test void testSelect() {
    final String sql = "select 1 from t";
    final String expected = "SELECT 1\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  @Test void testSel() {
    final String sql = "sel 1 from t";
    final String expected = "SELECT 1\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  @Test void testYearIsNotReserved() {
    final String sql = "select 1 as year from t";
    final String expected = "SELECT 1 AS `YEAR`\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  /** Tests that there are no reserved keywords. */
  @Disabled
  @Test void testKeywords() {
    final String[] reserved = {"AND", "ANY", "END-EXEC"};
    final StringBuilder sql = new StringBuilder("select ");
    final StringBuilder expected = new StringBuilder("SELECT ");
    for (String keyword : keywords(null)) {
      // Skip "END-EXEC"; I don't know how a keyword can contain '-'
      if (!Arrays.asList(reserved).contains(keyword)) {
        sql.append("1 as ").append(keyword).append(", ");
        expected.append("1 as `").append(keyword.toUpperCase(Locale.ROOT))
            .append("`,\n");
      }
    }
    sql.setLength(sql.length() - 2); // remove ', '
    expected.setLength(expected.length() - 2); // remove ',\n'
    sql.append(" from t");
    expected.append("\nFROM t");
    sql(sql.toString()).ok(expected.toString());
  }

  /** In Babel, AS is not reserved. */
  @Test void testAs() {
    final String expected = "SELECT `AS`\n"
        + "FROM `T`";
    sql("select as from t").ok(expected);
  }

  /** In Babel, DESC is not reserved. */
  @Test void testDesc() {
    final String sql = "select desc\n"
        + "from t\n"
        + "order by desc asc, desc desc";
    final String expected = "SELECT `DESC`\n"
        + "FROM `T`\n"
        + "ORDER BY `DESC`, `DESC` DESC";
    sql(sql).ok(expected);
  }

  /**
   * This is a failure test making sure the LOOKAHEAD for WHEN clause is 2 in Babel, where
   * in core parser this number is 1.
   *
   * @see SqlParserTest#testCaseExpression()
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-2847">[CALCITE-2847]
   * Optimize global LOOKAHEAD for SQL parsers</a>
   */
  @Test void testCaseExpressionBabel() {
    sql("case x when 2, 4 then 3 ^when^ then 5 else 4 end")
        .fails("(?s)Encountered \"when then\" at .*");
  }

  /** In Redshift, DATE is a function. It requires special treatment in the
   * parser because it is a reserved keyword.
   * (Curiously, TIMESTAMP and TIME are not functions.) */
  @Test void testDateFunction() {
    final String expected = "SELECT `DATE`(`X`)\n"
        + "FROM `T`";
    sql("select date(x) from t").ok(expected);
  }

  /** In Redshift, PostgreSQL the DATEADD, DATEDIFF and DATE_PART functions have
   * ordinary function syntax except that its first argument is a time unit
   * (e.g. DAY). We must not parse that first argument as an identifier. */
  @Test void testRedshiftFunctionsWithDateParts() {
    final String sql = "SELECT DATEADD(day, 1, t),\n"
        + " DATEDIFF(week, 2, t),\n"
        + " DATE_PART(year, t) FROM mytable";
    final String expected = "SELECT `DATEADD`(DAY, 1, `T`),"
        + " `DATEDIFF`(WEEK, 2, `T`), `DATE_PART`(YEAR, `T`)\n"
        + "FROM `MYTABLE`";

    sql(sql).ok(expected);
  }

  /** PostgreSQL and Redshift allow TIMESTAMP literals that contain only a
   * date part. */
  @Test void testShortTimestampLiteral() {
    sql("select timestamp '1969-07-20'")
        .ok("SELECT TIMESTAMP '1969-07-20 00:00:00'");
    // PostgreSQL allows the following. We should too.
    sql("select ^timestamp '1969-07-20 1:2'^")
        .fails("Illegal TIMESTAMP literal '1969-07-20 1:2': not in format "
            + "'yyyy-MM-dd HH:mm:ss'"); // PostgreSQL gives 1969-07-20 01:02:00
    sql("select ^timestamp '1969-07-20:23:'^")
        .fails("Illegal TIMESTAMP literal '1969-07-20:23:': not in format "
            + "'yyyy-MM-dd HH:mm:ss'"); // PostgreSQL gives 1969-07-20 23:00:00
  }

  /**
   * Babel parser's global {@code LOOKAHEAD} is larger than the core
   * parser's. This causes different parse error message between these two
   * parsers. Here we define a looser error checker for Babel, so that we can
   * reuse failure testing codes from {@link SqlParserTest}.
   *
   * <p>If a test case is written in this file -- that is, not inherited -- it
   * is still checked by {@link SqlParserTest}'s checker.
   */
  @Override protected Tester getTester() {
    return new TesterImpl() {
      @Override protected void checkEx(String expectedMsgPattern,
          SqlParserUtil.StringAndPos sap, Throwable thrown) {
        if (thrownByBabelTest(thrown)) {
          super.checkEx(expectedMsgPattern, sap, thrown);
        } else {
          checkExNotNull(sap, thrown);
        }
      }

      private boolean thrownByBabelTest(Throwable ex) {
        Throwable rootCause = Throwables.getRootCause(ex);
        StackTraceElement[] stackTrace = rootCause.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
          String className = stackTraceElement.getClassName();
          if (Objects.equals(className, BabelParserTest.class.getName())) {
            return true;
          }
        }
        return false;
      }

      private void checkExNotNull(SqlParserUtil.StringAndPos sap, Throwable thrown) {
        if (thrown == null) {
          throw new AssertionError("Expected query to throw exception, "
              + "but it did not; query [" + sap.sql
              + "]");
        }
      }
    };
  }

  /** Tests parsing PostgreSQL-style "::" cast operator. */
  @Test void testParseInfixCast()  {
    checkParseInfixCast("integer");
    checkParseInfixCast("varchar");
    checkParseInfixCast("boolean");
    checkParseInfixCast("double");
    checkParseInfixCast("bigint");

    final String sql = "select -('12' || '.34')::VARCHAR(30)::INTEGER as x\n"
        + "from t";
    final String expected = ""
        + "SELECT (- ('12' || '.34') :: VARCHAR(30) :: INTEGER) AS `X`\n"
        + "FROM `T`";
    sql(sql).ok(expected);
  }

  private void checkParseInfixCast(String sqlType) {
    String sql = "SELECT x::" + sqlType + " FROM (VALUES (1, 2)) as tbl(x,y)";
    String expected = "SELECT `X` :: " + sqlType.toUpperCase(Locale.ROOT) + "\n"
        + "FROM (VALUES (ROW(1, 2))) AS `TBL` (`X`, `Y`)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableWithNoSetTypeSpecified() {
    final String sql = "create table foo (bar integer not null, baz varchar(30))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateSetTable() {
    final String sql = "create set table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE SET TABLE `FOO` (`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMultisetTable() {
    final String sql = "create multiset table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE MULTISET TABLE `FOO` "
        + "(`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateVolatileTable() {
    final String sql = "create volatile table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE VOLATILE TABLE `FOO` "
        + "(`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTempTable() {
    final String sql = "create temp table foo (bar int not null, baz varchar(30))";
    final String expected = "CREATE TEMP TABLE `FOO` "
        + "(`BAR` INTEGER NOT NULL, `BAZ` VARCHAR(30))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableAsWithData() {
    final String sql = "create table foo as ( select * from bar ) with data";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "SELECT *\nFROM `BAR` WITH DATA";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableOnCommitPreserveRows() {
    final String sql = "create table foo (bar int) on commit preserve rows";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) ON COMMIT PRESERVE ROWS";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableOnCommitReleaseRows() {
    final String sql = "create table foo (bar int) on commit release rows";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) ON COMMIT RELEASE ROWS";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFallback() {
    final String sql = "create table foo, fallback (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FALLBACK (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFallbackOptionalParams() {
    final String sql = "create table foo, no fallback protection (bar integer)";
    final String expected = "CREATE TABLE `FOO`, NO FALLBACK PROTECTION (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTablePermutedColumnLevelAttributes() {
    final String sql = "create table foo (bar int uppercase null casespecific, "
        + "baz varchar(30) casespecific uppercase null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER UPPERCASE "
        + "CASESPECIFIC, `BAZ` VARCHAR(30) UPPERCASE CASESPECIFIC)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableNegatedColumnLevelAttributes() {
    final String sql = "create table foo (bar int not null not uppercase not "
        + "casespecific, baz varchar(30) not casespecific not uppercase not null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER NOT NULL NOT "
        + "UPPERCASE NOT CASESPECIFIC, `BAZ` VARCHAR(30) NOT NULL "
        + "NOT UPPERCASE NOT CASESPECIFIC)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableUppercaseColumnLevelAttribute() {
    final String sql = "create table foo (bar int uppercase)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER UPPERCASE)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCaseSpecificColumnLevelAttribute() {
    final String sql = "create table foo (bar int casespecific)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CASESPECIFIC)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableNullColumnLevelAttribute() {
    final String sql = "create table foo (bar int null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableNoColumnLevelAttribute() {
    final String sql = "create table foo (bar int)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testInsertWithSelectInParens() {
    final String sql = "insert into foo (SELECT * FROM bar)";
    final String expected = "INSERT INTO `FOO`\n"
        + "(SELECT *\nFROM `BAR`)";
    sql(sql).ok(expected);
  }

  @Test public void testInsertWithoutValuesKeywordSingleRow() {
    final String sql = "insert into foo (1,'hi')";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(1, 'hi'))";
    sql(sql).ok(expected);
  }

  @Test public void testInsertWithoutValuesKeywordMultipleRows() {
    final String sql = "insert into foo (1,'hi'), (2,'there')";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(1, 'hi')),\n"
        + "(ROW(2, 'there'))";
    sql(sql).ok(expected);
  }

  @Test public void testOrderByUnparsing() {
    final String sql =
        "(select 1 union all select Salary from Employee) order by Salary limit 1 offset 1";

    final String expected =
        "(SELECT 1\n"
            + "UNION ALL\n"
            + "SELECT `SALARY`\n"
            + "FROM `EMPLOYEE`)\n"
            + "ORDER BY `SALARY`\n"
            + "OFFSET 1 ROWS\n"
            + "FETCH NEXT 1 ROWS ONLY";
    sql(sql).ok(expected);
  }

  @Test public void testUpdateFromTable() {
    final String sql = "update foo from bar set foo.x = bar.y, foo.z = bar.k";
    final String expected = "UPDATE `FOO` FROM `BAR` SET `FOO`.`X` = `BAR`.`Y`, "
        + "`FOO`.`Z` = `BAR`.`K`";
    sql(sql).ok(expected);
  }

  @Test public void testUpdateFromTableWithAlias() {
    final String sql = "update foo as f from bar as b set f.x = b.y, f.z = b.k";
    final String expected = "UPDATE `FOO` AS `F` FROM `BAR` AS `B` SET `F`.`X` "
        + "= `B`.`Y`, `F`.`Z` = `B`.`K`";
    sql(sql).ok(expected);
  }

  @Test public void testUpdateFromTableBigQuery() {
    final String sql = "update foo from bar set foo.x = bar.y, foo.z = bar.k";
    final String expected = "UPDATE `foo` SET `foo`.`x` = `bar`.`y`, "
        + "`foo`.`z` = `bar`.`k` FROM `bar`";
    bigQuerySql(sql).ok(expected);
  }

  @Test public void testUpdateFromTableWithAliasBigQuery() {
    final String sql = "update foo as f set f.x = b.y, f.z = b.k";
    final String expected = "UPDATE `foo` AS `f` FROM `bar` AS `b` SET `f`.`x` "
        + "= `b`.`y`, `f`.`z` = `b`.`k` FROM `bar` AS `b`";
    bigQuerySql(sql).ok(expected);
  }
  @Test public void testExecMacro() {
    final String sql = "exec foo";
    final String expected = "EXECUTE `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacro() {
    final String sql = "execute foo";
    final String expected = "EXECUTE `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testUsingRequestModifierSingular() {
    final String sql = "using (foo int)";
    final String expected = "USING (`FOO` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testUsingRequestModifierMultiple() {
    final String sql = "using (foo int, bar varchar(30), baz int)";
    final String expected = "USING (`FOO` INTEGER, `BAR` VARCHAR(30), `BAZ` INTEGER)";
  }

  @Test public void testSetTimeZoneGMT() {
    final String sql = "set time zone \"GMT+10\"";
    final String expected = "SET TIME ZONE `GMT+10`";
    sql(sql).ok(expected);
  }

  @Test public void testSetTimeZoneColloquial() {
    final String sql = "set time zone \"Europe Moscow\"";
    final String expected = "SET TIME ZONE `Europe Moscow`";
    sql(sql).ok(expected);
  }

  @Test public void testCastFormatTime() {
    final String sql = "select cast('15h33m' as time(0) format 'HHhMIm')";
    final String expected = "SELECT CAST('15h33m' AS TIME(0) FORMAT 'HHhMIm')";
    sql(sql).ok(expected);
  }

  @Test public void testCastFormatDate() {
    final String sql = "select cast('2020-06-02' as date format 'yyyy-mm-dd')";
    final String expected = "SELECT CAST('2020-06-02' AS DATE FORMAT 'yyyy-mm-dd')";
    sql(sql).ok(expected);
  }

  @Test public void testInlineModSyntaxInteger() {
    final String sql = "select 27 mod -3";
    final String expected = "SELECT MOD(27, -3)";
    sql(sql).ok(expected);
  }

  @Test public void testInlineModSyntaxFloatingPoint() {
    final String sql = "select 27.123 mod 4.12";
    final String expected = "SELECT MOD(27.123, 4.12)";
    sql(sql).ok(expected);
  }

  @Test public void testInlineModSyntaxIdentifier() {
    final String sql = "select foo mod bar";
    final String expected = "SELECT MOD(`FOO`, `BAR`)";
    sql(sql).ok(expected);
  }
}
