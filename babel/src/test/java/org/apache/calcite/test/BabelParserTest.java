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

  @Test void testDel() {
    final String sql = "del from t";
    final String expected = "DELETE FROM `T`";
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
    final String expected = "SELECT DATE(`X`)\n"
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

  @Test public void testCreateOrReplaceTable() {
    final String sql = "create or replace table foo (bar integer)";
    final String expected = "CREATE OR REPLACE TABLE `FOO` (`BAR` INTEGER)";
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

  @Test public void testCreateTableWithSetTypeBeforeVolatility() {
    final String sql = "create multiset volatile table foo (bar integer)";
    final String expected = "CREATE MULTISET VOLATILE TABLE `FOO` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableWithVolatilityBeforeSetType() {
    final String sql = "create volatile multiset table foo (bar integer)";
    final String expected = "CREATE MULTISET VOLATILE TABLE `FOO` (`BAR` INTEGER)";
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

  @Test public void testCreateTableAsWithNoData() {
    final String sql = "create table foo as ( select * from bar ) with no data";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "SELECT *\nFROM `BAR` WITH NO DATA";
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

  @Test public void testTableAttributeJournalDefaultModifierDefaultType() {
    final String sql = "create table foo, journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalDefaultModifierBeforeType() {
    final String sql = "create table foo, before journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, BEFORE JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalDefaultModifierAfterType() {
    final String sql = "create table foo, after journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, AFTER JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalNoModifierBeforeType() {
    final String sql = "create table foo, no before journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, NO BEFORE JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalDualModifierBeforeType() {
    final String sql = "create table foo, dual before journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DUAL BEFORE JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalLocalModifierAfterType() {
    final String sql = "create table foo, local after journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, LOCAL AFTER JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalNotLocalModifierAfterType() {
    final String sql = "create table foo, not local after journal (bar integer)";
    final String expected = "CREATE TABLE `FOO`, NOT LOCAL AFTER JOURNAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalLocalModifierBeforeTypeFails() {
    final String sql = "create table foo, ^local^ before journal (bar integer)";
    final String expected = "(?s).*Encountered \"local before\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeJournalTableWithSimpleIdentifier() {
    final String sql = "create table foo, with journal table = baz (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH JOURNAL TABLE = `BAZ` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeJournalTableWithCompoundIdentifier() {
    final String sql = "create table foo, with journal table = baz.tbl (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH JOURNAL TABLE = `BAZ`.`TBL` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMap() {
    final String sql = "create table foo, map = baz (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MAP = `BAZ` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpace() {
    final String sql = "create table foo, freespace = 35 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FREESPACE = 35 (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpacePercent() {
    final String sql = "create table foo, freespace = 35 percent (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FREESPACE = 35 PERCENT (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpaceTruncated() {
    final String sql = "create table foo, freespace = 32.65 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FREESPACE = 32 (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpaceNegativeFails() {
    final String sql = "create table foo, freespace = ^-^32.65 (bar integer)";
    final String expected = "(?s).*Encountered \"-\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeFreeSpaceOutOfRangeFails() {
    final String sql = "create table foo, freespace = ^82.5^ (bar integer)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeFreeSpaceOutOfRangePercentFails() {
    final String sql = "create table foo, freespace = ^82.5^ percent (bar integer)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeFreeSpaceRangeLowerBound() {
    final String sql = "create table foo, freespace = 0 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FREESPACE = 0 (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpaceRangeBelowLowerBoundFail() {
    final String sql = "create table foo, freespace = ^-^1 (bar integer)";
    final String expected = "(?s).*Encountered \"-\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeFreeSpaceRangeUpperBound() {
    final String sql = "create table foo, freespace = 75 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, FREESPACE = 75 (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeFreeSpaceRangeAboveUpperBound() {
    final String sql = "create table foo, freespace = ^76^ (bar integer)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingDefault() {
    final String sql = "create table foo, with isolated loading (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH ISOLATED LOADING (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingNoModifier() {
    final String sql = "create table foo, with no isolated loading (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH NO ISOLATED LOADING (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingConcurrentModifier() {
    final String sql = "create table foo, with concurrent isolated loading (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH CONCURRENT ISOLATED LOADING (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingNoConcurrentModifier() {
    final String sql = "create table foo, with no concurrent isolated loading (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH NO CONCURRENT ISOLATED LOADING "
        + "(`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingForAllOperationLevel() {
    final String sql = "create table foo, with isolated loading for all (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH ISOLATED LOADING FOR ALL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingForInsertOperationLevel() {
    final String sql = "create table foo, with isolated loading for insert (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH ISOLATED LOADING FOR INSERT (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingForNoneOperationLevel() {
    final String sql = "create table foo, with isolated loading for none (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH ISOLATED LOADING FOR NONE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeIsolatedLoadingModifierOperationLevel() {
    final String sql = "create table foo, with concurrent isolated loading for none (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH CONCURRENT ISOLATED LOADING FOR NONE "
        + "(`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeMinimum() {
    final String sql = "create table foo, minimum datablocksize (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MINIMUM DATABLOCKSIZE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeMin() {
    final String sql = "create table foo, min datablocksize (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MINIMUM DATABLOCKSIZE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeMaximum() {
    final String sql = "create table foo, maximum datablocksize (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MAXIMUM DATABLOCKSIZE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeMax() {
    final String sql = "create table foo, max datablocksize (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MAXIMUM DATABLOCKSIZE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeDefault() {
    final String sql = "create table foo, default datablocksize (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DEFAULT DATABLOCKSIZE (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeValueNoUnit() {
    final String sql = "create table foo, datablocksize = 12123 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DATABLOCKSIZE = 12123 BYTES (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeValueBytes() {
    final String sql = "create table foo, datablocksize = 12123 bytes (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DATABLOCKSIZE = 12123 BYTES (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeValueKbytes() {
    final String sql = "create table foo, datablocksize = 42.123 kbytes (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DATABLOCKSIZE = 42.123 KILOBYTES (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeValueKilobytes() {
    final String sql = "create table foo, datablocksize = 2e4 kilobytes (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DATABLOCKSIZE = 2E4 KILOBYTES (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeDataBlockSizeModifierValueFails() {
    final String sql = "create table foo, max datablocksize ^=^ 2e4 kilobytes (bar integer)";
    final String expected = "(?s)Encountered \"=\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeChecksumDefault() {
    final String sql = "create table foo, checksum = default (bar integer)";
    final String expected = "CREATE TABLE `FOO`, CHECKSUM = DEFAULT (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeChecksumOn() {
    final String sql = "create table foo, checksum = on (bar integer)";
    final String expected = "CREATE TABLE `FOO`, CHECKSUM = ON (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeChecksumOff() {
    final String sql = "create table foo, checksum = off (bar integer)";
    final String expected = "CREATE TABLE `FOO`, CHECKSUM = OFF (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeBlockCompressionDefault() {
    final String sql = "create table foo, blockcompression = default (bar integer)";
    final String expected = "CREATE TABLE `FOO`, BLOCKCOMPRESSION = DEFAULT (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeBlockCompressionAutotemp() {
    final String sql = "create table foo, blockcompression = autotemp (bar integer)";
    final String expected = "CREATE TABLE `FOO`, BLOCKCOMPRESSION = AUTOTEMP (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeBlockCompressionManual() {
    final String sql = "create table foo, blockcompression = manual (bar integer)";
    final String expected = "CREATE TABLE `FOO`, BLOCKCOMPRESSION = MANUAL (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeBlockCompressionNever() {
    final String sql = "create table foo, blockcompression = never (bar integer)";
    final String expected = "CREATE TABLE `FOO`, BLOCKCOMPRESSION = NEVER (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeLog() {
    final String sql = "create table foo, log (bar integer)";
    final String expected = "CREATE TABLE `FOO`, LOG (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeLogNoModifier() {
    final String sql = "create table foo, no log (bar integer)";
    final String expected = "CREATE TABLE `FOO`, NO LOG (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMultipleAttrs() {
    final String sql = "create table foo, with journal table = baz.tbl, "
        + "fallback protection, checksum = on (bar integer)";
    final String expected = "CREATE TABLE `FOO`, WITH JOURNAL TABLE = `BAZ`.`TBL`, "
        + "FALLBACK PROTECTION, CHECKSUM = ON (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMultipleAttrsOrder() {
    final String sql = "create table foo, checksum = on, fallback protection, "
        + "with journal table = baz.tbl (bar integer)";
    final String expected = "CREATE TABLE `FOO`, CHECKSUM = ON, FALLBACK PROTECTION, "
        + "WITH JOURNAL TABLE = `BAZ`.`TBL` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioDefault() {
    final String sql = "create table foo, default mergeblockratio (bar integer)";
    final String expected = "CREATE TABLE `FOO`, DEFAULT MERGEBLOCKRATIO (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioNo() {
    final String sql = "create table foo, no mergeblockratio (bar integer)";
    final String expected = "CREATE TABLE `FOO`, NO MERGEBLOCKRATIO (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioInteger() {
    final String sql = "create table foo, mergeblockratio = 45 (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MERGEBLOCKRATIO = 45 (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioIntegerNegativeFails() {
    final String sql = "create table foo, mergeblockratio = ^-^20 (bar integer)";
    final String expected = "(?s).*Encountered \"-\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioIntegerRangeFails() {
    final String sql = "create table foo, mergeblockratio = ^155^ (bar integer)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioPercent() {
    final String sql = "create table foo, mergeblockratio = 45 percent (bar integer)";
    final String expected = "CREATE TABLE `FOO`, MERGEBLOCKRATIO = 45 PERCENT (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testTableAttributeMergeBlockRatioNoModifierIntegerFails() {
    final String sql = "create table foo, no mergeblockratio ^=^ 45 percent (bar integer)";
    final String expected = "(?s).*Encountered \"=\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testCreateTablePermutedColumnLevelAttributes() {
    final String sql = "create table foo (bar int uppercase null casespecific, "
        + "baz varchar(30) casespecific uppercase null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER UPPERCASE "
        + "CASESPECIFIC, `BAZ` VARCHAR(30) CASESPECIFIC UPPERCASE)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableNegatedColumnLevelAttributes() {
    final String sql = "create table foo (bar int not null not uppercase not "
        + "casespecific, baz varchar(30) not casespecific not uppercase not null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER NOT NULL NOT "
        + "UPPERCASE NOT CASESPECIFIC, `BAZ` VARCHAR(30) NOT NULL "
        + "NOT CASESPECIFIC NOT UPPERCASE)";
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

  @Test public void testCreateTableCompressColumnLevelAttribute() {
    final String sql = "create table foo (bar int compress)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithIntegersColumnLevelAttribute() {
    final String sql = "create table foo (bar int compress (1, 2))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS (1, 2))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithStringsColumnLevelAttribute() {
    final String sql = "create table foo (bar int compress ('a', 'b'))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS ('a', 'b'))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithDatesColumnLevelAttribute() {
    final String sql = "create table foo (bar int compress ("
        + "date '1972-02-28', date '1972-02-29'))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS ("
        + "DATE '1972-02-28', DATE '1972-02-29'))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithTimestampsColumnLevelAttribute() {
    final String sql = "create table foo (bar int compress ("
        + "timestamp '2006-11-23 15:30:23', timestamp '2006-11-23 15:30:24'))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS ("
        + "TIMESTAMP '2006-11-23 15:30:23', TIMESTAMP '2006-11-23 15:30:24'))";
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

  @Test public void testCreateTableLatinCharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set latin)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET LATIN)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableUnicodeCharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set unicode)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET UNICODE)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableGraphicCharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set graphic)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET GRAPHIC)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableKanjisjisCharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set kanjisjis)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER "
        + "SET KANJISJIS)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableKanjiCharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set kanji)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET KANJI)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCharacterSetAndUppercaseColumnLevelAttributes() {
    final String sql = "create table foo (bar int character set kanji uppercase)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET "
        + "KANJI UPPERCASE)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultNumericColumnLevelAttribute() {
    final String sql = "create table foo (bar int default 1)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultStringColumnLevelAttribute() {
    final String sql = "create table foo (bar int default 'baz')";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT 'baz')";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultNullColumnLevelAttribute() {
    final String sql = "create table foo (bar int default null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT NULL)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultUserColumnLevelAttribute() {
    final String sql = "create table foo (bar int default user)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT USER)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultCurrentDateColumnLevelAttribute() {
    final String sql = "create table foo (bar int default current_date)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT CURRENT_DATE)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultCurrentTimeColumnLevelAttribute() {
    final String sql = "create table foo (bar int default current_time)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT CURRENT_TIME)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultCurrentTimestampColumnLevelAttribute() {
    final String sql = "create table foo (bar int default current_timestamp)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT CURRENT_TIMESTAMP)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultCurrentDateWithParamColumnLevelAttribute() {
    final String sql = "create table foo (bar int default current_date(0))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT CURRENT_DATE(0))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableDefaultCaseSpecificColumnLevelAttributes() {
    final String sql = "create table foo (bar int default 1 casespecific)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER DEFAULT 1 CASESPECIFIC)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableWithDateFormatStringColumnLevelAttribute() {
    final String sql = "create table foo (bar date format 'YYYY-MM-DD')";
    final String expected = "CREATE TABLE `FOO` (`BAR` DATE FORMAT 'YYYY-MM-DD')";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableWithDateFormatStringAndOtherColumnLevelAttributes() {
    final String sql = "create table foo (bar date format 'YYYY-MM-DD'"
        + " default null)";
    final String expected = "CREATE TABLE `FOO` (`BAR` DATE FORMAT 'YYYY-MM-DD'"
        + " DEFAULT NULL)";
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

  @Test public void testIns() {
    final String sql = "ins into foo (1,'hi')";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(1, 'hi'))";
    sql(sql).ok(expected);
  }

  @Test void testBigQueryUnicodeUnparsing() {
    final String sql = "SELECT '¶ÑÍ·'";
    final String expected = "SELECT '\\u00b6\\u00d1\\u00cd\\u00b7'";
    sql(sql)
      .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
      .ok(expected);
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

  @Test void testUpd() {
    final String sql = "upd foo from bar set foo.x = bar.y, foo.z = bar.k";
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
    final String expected = "UPDATE foo SET foo.x = bar.y, "
        + "foo.z = bar.k FROM bar";
    sql(sql)
      .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
      .ok(expected);
  }

  @Test public void testUpdateFromTableWithAliasBigQuery() {
    final String sql = "update foo as f from bar as b set f.x = b.y, f.z = b.k";
    final String expected = "UPDATE foo AS f SET f.x "
        + "= b.y, f.z = b.k FROM bar AS b";
    sql(sql)
      .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
      .ok(expected);
  }

  @Test public void testUpdateFromMultipleSources() {
    final String sql = "UPDATE foo from bar, baz SET foo.x = bar.x, foo.y = baz.y";
    final String expected = "UPDATE `FOO` FROM `BAR`, `BAZ` SET "
        + "`FOO`.`X` = `BAR`.`X`, `FOO`.`Y` = `BAZ`.`Y`";
    sql(sql).ok(expected);
  }

  @Test public void testUpdateFromMultipleSourcesBigQuery() {
    final String sql = "UPDATE foo FROM bar, baz SET foo.x = bar.x, foo.y = baz.y";
    final String expected = "UPDATE foo SET foo.x = bar.x, foo.y = baz.y FROM bar, baz";
    sql(sql)
      .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
      .ok(expected);
  }

  @Test public void testUpdateFromMultipleSourcesAllAliased() {
    final String sql = "UPDATE foo as f from bar as b, baz as z SET "
        + "f.x = b.x, f.y = z.y";
    final String expected = "UPDATE `FOO` AS `F` FROM `BAR` AS `B`, `BAZ` AS `Z` SET "
        + "`F`.`X` = `B`.`X`, `F`.`Y` = `Z`.`Y`";
    sql(sql).ok(expected);
  }

  @Test public void testUpdateFromMultipleSourcesSomeAliased() {
    final String sql = "UPDATE foo as f from bar as b, baz, qux as q SET "
        + "f.x = b.x, f.y = baz.y, f.z = q.z";
    final String expected = "UPDATE `FOO` AS `F` FROM `BAR` AS `B`, `BAZ`, "
        + "`QUX` AS `Q` SET `F`.`X` = `B`.`X`, `F`.`Y` = `BAZ`.`Y`, `F`.`Z` = `Q`.`Z`";
    sql(sql).ok(expected);
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

  @Test public void testDateTimePrimaryLiteral() {
    final String sql = "select timestamp '2020-05-30 13:20:00'";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00'";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryBuiltInFunction() {
    final String sql = "select current_timestamp";
    final String expected = "SELECT CURRENT_TIMESTAMP";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryLiteralAtLocal() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at local";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT LOCAL";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryColumnAtLocal() {
    final String sql = "select foo at local";
    final String expected = "SELECT `FOO` AT LOCAL";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryBuiltInFunctionAtLocal() {
    final String sql = "select current_timestamp at local";
    final String expected = "SELECT CURRENT_TIMESTAMP AT LOCAL";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryLiteralAtTimeZone() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at time zone \"GMT+10\"";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT TIME ZONE `GMT+10`";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryColumnAtTimeZone() {
    final String sql = "select foo at time zone \"Europe Moscow\"";
    final String expected = "SELECT `FOO` AT TIME ZONE `Europe Moscow`";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryBuiltInFunctionAtTimeZone() {
    final String sql = "select current_date at time zone \"GMT-5\"";
    final String expected = "SELECT CURRENT_DATE AT TIME ZONE `GMT-5`";
    sql(sql).ok(expected);
  }

  @Test public void testDateTimePrimaryColumnAtLocalInWhereClause() {
    final String sql =
        "select foo "
        + "from fooTable "
        + "where timestamp '2020-05-30 13:20:00' at local = timestamp '2020-05-30 20:20:00'";
    final String expected =
        "SELECT `FOO`\n"
        + "FROM `FOOTABLE`\n"
        + "WHERE (TIMESTAMP '2020-05-30 13:20:00' AT LOCAL = TIMESTAMP '2020-05-30 20:20:00')";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementValidInterval() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at time zone "
        + "interval '1:20' minute to second";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT "
        + "TIME ZONE INTERVAL '1:20' MINUTE TO SECOND";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementValidIntervalWithoutTimeZone() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at "
        + "interval '1:20' minute to second";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT "
        + "TIME ZONE INTERVAL '1:20' MINUTE TO SECOND";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementValidBigIntPrecisionZero() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at "
        + "9223372036854775807";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT "
        + "TIME ZONE 9223372036854775807";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementValidDecimalPrecisionZeroNegative() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at time zone "
        + "-2";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT "
        + "TIME ZONE -2";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementValidDecimalPrecisionGreaterThanZero() {
    final String sql = "select timestamp '2020-05-30 13:20:00' at time zone "
        + "1.5";
    final String expected = "SELECT TIMESTAMP '2020-05-30 13:20:00' AT "
        + "TIME ZONE 1.5";
    sql(sql).ok(expected);
  }

  @Test public void testAtTimeZoneDisplacementNonUnicodeString() {
    final String sql = "select foo at time zone \"Hădrĭa\"";
    final String expected = "SELECT `FOO` AT TIME ZONE `Hădrĭa`";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormNoParameter() {
    final String sql =
        "create function foo() "
        + "returns Integer "
        + "language sql "
        + "collation invoker inline type 1 "
        + "return current_date + 1";
    final String expected = "CREATE FUNCTION `FOO` () "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (CURRENT_DATE + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormOneParameter() {
    final String sql =
        "create function foo(a Integer) "
            + "returns Integer "
            + "language sql "
            + "collation invoker inline type 1 "
            + "return current_date + 1";
    final String expected = "CREATE FUNCTION `FOO` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (CURRENT_DATE + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormNotIncludeLanguageSQLFails() {
    final String sql =
        "create function foo() "
            + "returns Integer "
            + "^collation^ invoker inline type 1 "
            + "return current_date + 1";
    final String expected = "(?s).*Encountered \"collation\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testCreateFunctionSqlFormMoreThanOneParameter() {
    final String sql =
        "create function foo(a Integer, b varchar(30) ) "
            + "returns Integer "
            + "language sql "
            + "collation invoker inline type 1 "
            + "return current_date + 1";
    final String expected = "CREATE FUNCTION `FOO` (`A` INTEGER, `B` VARCHAR(30)) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (CURRENT_DATE + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormNotDeterministic() {
    final String sql =
        "create function add1(a Integer) "
            + "returns Integer "
            + "language sql "
            + "not deterministic "
            + "collation invoker inline type 1 "
            + "return a + 1";
    final String expected = "CREATE FUNCTION `ADD1` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "NOT DETERMINISTIC "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (`A` + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormDeterministic() {
    final String sql =
        "create function add1(a Integer) "
            + "returns Integer "
            + "language sql "
            + "deterministic "
            + "collation invoker inline type 1 "
            + "return a + 1";
    final String expected = "CREATE FUNCTION `ADD1` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "DETERMINISTIC "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (`A` + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormCalledOnNullInput() {
    final String sql =
        "create function add1(a Integer) "
            + "returns Integer "
            + "language sql "
            + "called on null input "
            + "collation invoker inline type 1 "
            + "return a + 1";
    final String expected = "CREATE FUNCTION `ADD1` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "CALLED ON NULL INPUT "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (`A` + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormReturnsNullOnNullInput() {
    final String sql =
        "create function add1(a Integer) "
            + "returns Integer "
            + "language sql "
            + "returns null on null input "
            + "collation invoker inline type 1 "
            + "return a + 1";
    final String expected = "CREATE FUNCTION `ADD1` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "RETURNS NULL ON NULL INPUT "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (`A` + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormSpecificFunctionName() {
    final String sql =
        "create function add1(a Integer) "
            + "returns Integer "
            + "language sql "
            + "specific someTable.plusOne "
            + "collation invoker inline type 1 "
            + "return a + 1";
    final String expected = "CREATE FUNCTION `ADD1` (`A` INTEGER) "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "SPECIFIC `SOMETABLE`.`PLUSONE` "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (`A` + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormInvalidPositiveTypeIntFails() {
    final String sql =
        "create function foo() "
            + "returns Integer "
            + "language sql "
            + "collation invoker inline type ^2^ "
            + "return current_date + 1";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testCreateFunctionSqlFormInvalidNegativeTypeIntFails() {
    final String sql =
        "create function foo() "
            + "returns Integer "
            + "language sql "
            + "collation invoker inline type ^-^2 "
            + "return current_date + 1";
    final String expected = "(?s).*Encountered \"-\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testCreateFunctionSqlFormHasSecurityDefiner() {
    final String sql =
        "create function foo() "
            + "returns Integer "
            + "language sql "
            + "sql security definer "
            + "collation invoker inline type 1 "
            + "return current_date + 1";
    final String expected = "CREATE FUNCTION `FOO` () "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "SQL SECURITY DEFINER "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (CURRENT_DATE + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateFunctionSqlFormNotDeterministicReturnsNullHasSecurityDefiner() {
    final String sql =
        "create function foo() "
            + "returns Integer "
            + "language sql "
            + "returns null on null input "
            + "not deterministic "
            + "sql security definer "
            + "collation invoker inline type 1 "
            + "return current_date + 1";
    final String expected = "CREATE FUNCTION `FOO` () "
        + "RETURNS INTEGER "
        + "LANGUAGE SQL "
        + "NOT DETERMINISTIC "
        + "RETURNS NULL ON NULL INPUT "
        + "SQL SECURITY DEFINER "
        + "COLLATION INVOKER INLINE TYPE 1 "
        + "RETURN (CURRENT_DATE + 1)";
    sql(sql).ok(expected);
  }

  @Test public void testTranslateUsingCharacterSet() {
    expr("translate ('abc' using latin_to_unicode)")
        .ok("TRANSLATE ('abc' USING LATIN_TO_UNICODE)");
  }

  @Test public void testTranslateUsingCharacterSetWithError() {
    expr("translate ('abc' using latin_to_unicode with error)")
        .ok("TRANSLATE ('abc' USING LATIN_TO_UNICODE WITH ERROR)");
  }

  @Test public void testTranslateUsingCharacterSetWithErrorInSelectStatement() {
    final String sql = "SELECT TRANSLATE('bar' USING LATIN_TO_UNICODE WITH ERROR) bar_translated "
        + "FROM foo";
    final String expected =
        "SELECT TRANSLATE ('bar' USING LATIN_TO_UNICODE WITH ERROR) AS `BAR_TRANSLATED`\n"
            + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testTranslateOriginalUseCaseParsedToWithoutErrorToken() {
    final String sql = "translate('abc' using lazy_translation with error)";
    final String expected = "TRANSLATE('abc' USING `LAZY_TRANSLATION`)";
    expr(sql).ok(expected);
  }

  @Test public void testUsingRequestModifierSingular() {
    final String sql = "using (foo int)";
    final String expected = "USING (`FOO` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test public void testUsingRequestModifierMultiple() {
    final String sql = "using (foo int, bar varchar(30), baz int)";
    final String expected = "USING (`FOO` INTEGER, `BAR` VARCHAR(30), `BAZ` INTEGER)";
    sql(sql).ok(expected);
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

  @Test public void testNamedExpressionAlone() {
    final String sql = "select (a + b) (named x) from foo where x > 0";
    final String expected = "SELECT (`A` + `B`) AS `X`\n"
        + "FROM `FOO`\n"
        + "WHERE (`X` > 0)";
    sql(sql).ok(expected);
  }

  @Test public void testNamedExpressionWithOtherAttributes() {
    final String sql = "select (a + b) (named x), k from foo where x > 0";
    final String expected = "SELECT (`A` + `B`) AS `X`, `K`\n"
        + "FROM `FOO`\n"
        + "WHERE (`X` > 0)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedNamedExpression() {
    final String sql = "SELECT (((a + b) (named x)) + y) (named z) from foo "
        + "where z > 0 and x > 0";
    final String expected = "SELECT (((`A` + `B`) AS `X`) + `Y`) AS `Z`\n"
        + "FROM `FOO`\n"
        + "WHERE ((`Z` > 0) AND (`X` > 0))";
    sql(sql).ok(expected);
  }

  @Test void testNestedSchemaAccess() {
    final String sql = "SELECT a.b.c.d.column";
    final String expected = "SELECT `A`.`B`.`C`.`D`.`COLUMN`";
    sql(sql).ok(expected);
  }

  @Test void testBetweenUnparsing() {
    final String sql = "SELECT * FROM foo WHERE col BETWEEN 1 AND 3";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`COL` BETWEEN 1 AND 3)";
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

  @Test public void testDateAtLocalWhere() {
    final String sql = "SELECT * FROM foo WHERE bar = DATE AT LOCAL";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` = DATE AT LOCAL)";
    sql(sql).ok(expected);
  }

  @Test public void testDateAtLocalSelect() {
    final String sql = "SELECT DATE AT LOCAL";
    final String expected = "SELECT DATE AT LOCAL";
    sql(sql).ok(expected);
  }

  @Test public void testSelectDateExpression() {
    final String sql = "SELECT DATE -1 FROM foo";
    final String expected = "SELECT (DATE - 1)\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testCurrentTimestampFunction() {
    final String sql = "select current_timestamp";
    final String expected = "SELECT CURRENT_TIMESTAMP";
    sql(sql).ok(expected);
  }

  @Test public void testCurrentTimeFunction() {
    final String sql = "select current_time";
    final String expected = "SELECT CURRENT_TIME";
    sql(sql).ok(expected);
  }

  @Test public void testCurrentDateFunction() {
    final String sql = "select current_date";
    final String expected = "SELECT CURRENT_DATE";
    sql(sql).ok(expected);
  }

  @Test public void testMultipleTimeFunctions() {
    final String sql = "select current_time, current_timestamp";
    final String expected = "SELECT CURRENT_TIME, CURRENT_TIMESTAMP";
    sql(sql).ok(expected);
  }

  @Test public void testMergeInto() {
    final String sql = "merge into t1 a using t2 b on a.x = b.x when matched then "
        + "update set y = b.y when not matched then insert (x,y) values (b.x, b.y)";
    final String expected = "MERGE INTO `T1` AS `A`\n"
        + "USING `T2` AS `B`\n"
        + "ON (`A`.`X` = `B`.`X`)\n"
        + "WHEN MATCHED THEN UPDATE SET `Y` = `B`.`Y`\n"
        + "WHEN NOT MATCHED THEN INSERT (`X`, `Y`) (VALUES (ROW(`B`.`X`, `B`.`Y`)))";
    sql(sql).ok(expected);
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

  @Test void testIfTokenIsQuotedInAnsi() {
    final String sql = "select if(x) from foo";
    final String expected = "SELECT `IF`(`X`)\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testIfTokenIsNotQuotedInBigQuery() {
    final String sql = "select if(x) from foo";
    final String expected = "SELECT if(x)\n"
        + "FROM foo";
    sql(sql)
        .withDialect(SqlDialect.DatabaseProduct.BIG_QUERY.getDialect())
        .ok(expected);
  }

  @Test void testUpsertAllOptionalSpecified() {
    final String sql = "UPDATE foo SET x = 1 WHERE x > 1 ELSE INSERT INTO"
        + " bar (x) VALUES (1)";
    final String expected = "UPDATE `FOO` SET `X` = 1\n"
        + "WHERE (`X` > 1) ELSE INSERT INTO `BAR` (`X`)\n"
        + "VALUES (ROW(1))";
    sql(sql).ok(expected);
  }

  @Test void testUpsertAllOptionalOmitted() {
    final String sql = "UPDATE foo SET x = 1 WHERE x > 1 ELSE INSERT bar (1)";
    final String expected = "UPDATE `FOO` SET `X` = 1\n"
        + "WHERE (`X` > 1) ELSE INSERT INTO `BAR`\n"
        + "VALUES (ROW(1))";
    sql(sql).ok(expected);
  }

  @Test void testUpsertWithUpdKeyword() {
    final String sql = "UPD foo SET x = 1 WHERE x > 1 ELSE INSERT bar (1)";
    final String expected = "UPDATE `FOO` SET `X` = 1\n"
        + "WHERE (`X` > 1) ELSE INSERT INTO `BAR`\n"
        + "VALUES (ROW(1))";
    sql(sql).ok(expected);
  }

  @Test void testUpsertWithInsKeyword() {
    final String sql = "UPDATE foo SET x = 1 WHERE x > 1 ELSE INS bar (1)";
    final String expected = "UPDATE `FOO` SET `X` = 1\n"
        + "WHERE (`X` > 1) ELSE INSERT INTO `BAR`\n"
        + "VALUES (ROW(1))";
    sql(sql).ok(expected);
  }

  @Test public void testSubstr() {
    final String sql = "select substr('FOOBAR' from 1 for 3)";
    final String expected = "SELECT SUBSTRING('FOOBAR' FROM 1 FOR 3)";
    sql(sql).ok(expected);
  }

  @Test public void testPrimaryIndexNoName() {
    final String sql = "create table foo primary index (lname)";
    final String expected = "CREATE TABLE `FOO` PRIMARY INDEX (`LNAME`)";
    sql(sql).ok(expected);
  }

  @Test public void testPrimaryIndexWithName() {
    final String sql = "create table foo primary index bar (lname)";
    final String expected = "CREATE TABLE `FOO` PRIMARY INDEX `BAR` (`LNAME`)";
    sql(sql).ok(expected);
  }

  @Test public void testPrimaryIndexMultiColumn() {
    final String sql = "create table foo primary index bar (lname, fname)";
    final String expected = "CREATE TABLE `FOO` PRIMARY INDEX `BAR` (`LNAME`, `FNAME`)";
    sql(sql).ok(expected);
  }

  @Test public void testPrimaryIndexUnique() {
    final String sql = "create table foo unique primary index (lname)";
    final String expected = "CREATE TABLE `FOO` UNIQUE PRIMARY INDEX (`LNAME`)";
    sql(sql).ok(expected);
  }

  @Test public void testNoPrimaryIndex() {
    final String sql = "create table foo no primary index";
    final String expected = "CREATE TABLE `FOO` NO PRIMARY INDEX";
    sql(sql).ok(expected);
  }

  @Test public void testPrimaryIndexMultipleOverwritten() {
    final String sql = "create table foo primary index bar (lname), primary index baz (fname)";
    final String expected = "CREATE TABLE `FOO` PRIMARY INDEX `BAZ` (`FNAME`)";
    sql(sql).ok(expected);
  }

  @Test public void testNoPrimaryIndexWithColumnsFails() {
    final String sql = "create table foo no primary index ^(^lname)";
    final String expected = "(?s).*Encountered \"\\(\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testNoPrimaryIndexWithNameFails() {
    final String sql = "create table foo no primary index ^bar^";
    final String expected = "(?s).*Encountered \"bar\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testPrimaryIndexNoColumnsFails() {
    final String sql = "create table foo primary inde^x^";
    final String expected = "(?s).*Encountered \"<EOF>\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testQualify() {
    final String sql = "select count(foo) as bar from baz qualify bar = 5";
    final String expected = "SELECT COUNT(`FOO`) AS `BAR`\n"
        + "FROM `BAZ`\n"
        + "QUALIFY (`BAR` = 5)";
    sql(sql).ok(expected);
  }

  @Test public void testQualifyNestedExpression() {
    final String sql = "select count(foo) as x from bar qualify x in (select y from baz)";
    final String expected = "SELECT COUNT(`FOO`) AS `X`\n"
        + "FROM `BAR`\n"
        + "QUALIFY (`X` IN (SELECT `Y`\n"
        + "FROM `BAZ`))";
    sql(sql).ok(expected);
  }

  @Test public void testQualifyWithSurroundingClauses() {
    final String sql = "select count(foo) as x, sum(y), z from bar where z > 5 "
        + "having y < 5 qualify x = 5 order by z";
    final String expected = "SELECT COUNT(`FOO`) AS `X`, SUM(`Y`), `Z`\n"
        + "FROM `BAR`\n"
        + "WHERE (`Z` > 5)\n"
        + "HAVING (`Y` < 5)\n"
        + "QUALIFY (`X` = 5)\n"
        + "ORDER BY `Z`";
    sql(sql).ok(expected);
  }

  @Test public void testRenameTableWithTo() {
    final String sql = "rename table foo to bar";
    final String expected = "RENAME TABLE `FOO` TO `BAR`";
    sql(sql).ok(expected);
  }

  @Test public void testRenameTableWithAs() {
    final String sql = "rename table foo as bar";
    final String expected = "RENAME TABLE `FOO` AS `BAR`";
    sql(sql).ok(expected);
  }

  @Test public void testRenameTableWithCompoundIdentifiers() {
    final String sql = "rename table foo.bar as bar.foo";
    final String expected = "RENAME TABLE `FOO`.`BAR` AS `BAR`.`FOO`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionIdentifier() {
    final String sql = "select foo (integer)";
    final String expected = "SELECT CAST(`FOO` AS INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionLiteral() {
    final String sql = "select 12.5 (integer)";
    final String expected = "SELECT CAST(12.5 AS INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionFormat() {
    final String sql = "select '15h33m' (time(0) format 'HHhMIm')";
    final String expected = "SELECT CAST('15h33m' AS TIME(0) FORMAT 'HHhMIm')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionInterval() {
    final String sql = "select '3700 sec' (interval minute)";
    final String expected = "SELECT CAST('3700 sec' AS INTERVAL MINUTE)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionQuery() {
    final String sql = "select (select foo from bar) (integer) from baz";
    final String expected = "SELECT CAST((SELECT `FOO`\n"
        + "FROM `BAR`) AS INTEGER)\n"
        + "FROM `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionQueryFormat() {
    final String sql = "select (select foo from bar) (time(0) format 'HHhMIm') from baz";
    final String expected = "SELECT CAST((SELECT `FOO`\n"
        + "FROM `BAR`) AS TIME(0) FORMAT 'HHhMIm')\n"
        + "FROM `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionQueryInterval() {
    final String sql = "select (select foo from bar) (interval minute) from baz";
    final String expected = "SELECT CAST((SELECT `FOO`\n"
        + "FROM `BAR`) AS INTERVAL MINUTE)\n"
        + "FROM `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionQueryNested() {
    final String sql = "select (select foo (integer) from bar) (char) from baz";
    final String expected = "SELECT CAST((SELECT CAST(`FOO` AS INTEGER)\n"
        + "FROM `BAR`) AS CHAR)\n"
        + "FROM `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableAsIdentifier() {
    final String sql = "create table foo as bar";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "`BAR`";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableAsCompoundIdentifier() {
    final String sql = "create table foo as bar.baz";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "`BAR`.`BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableAsIdentifierWithData() {
    final String sql = "create table foo as bar with data";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "`BAR` WITH DATA";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableAsIdentifierWithNoData() {
    final String sql = "create table foo as bar with no data";
    final String expected = "CREATE TABLE `FOO` AS\n"
        + "`BAR` WITH NO DATA";
    sql(sql).ok(expected);
  }

  @Test void testRankAnsiSyntax() {
    final String sql = "select rank() over (order by foo desc) from bar";
    final String expected = "SELECT (RANK() OVER (ORDER BY `FOO` DESC))\n"
        + "FROM `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testRankSortingParam() {
    final String sql = "select rank(foo) from bar";
    final String expected = "SELECT (RANK() OVER (ORDER BY `FOO` DESC))\n"
        + "FROM `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testRankSortingParamAsc() {
    final String sql = "select rank(foo asc) from bar";
    final String expected = "SELECT (RANK() OVER (ORDER BY `FOO`))\n"
        + "FROM `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testRankSortingParamDesc() {
    final String sql = "select rank(foo desc) from bar";
    final String expected = "SELECT (RANK() OVER (ORDER BY `FOO` DESC))\n"
        + "FROM `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testRankSortingParamCommaList() {
    final String sql = "select rank(foo asc, baz desc, x) from bar";
    final String expected = "SELECT (RANK() OVER (ORDER BY `FOO`, `BAZ` DESC, `X` DESC))\n"
        + "FROM `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testIndexWithoutName() {
    final String sql = "create table foo (bar integer) index (bar)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) INDEX (`BAR`)";
    sql(sql).ok(expected);
  }

  @Test void testIndexWithName() {
    final String sql = "create table foo (bar integer) index baz (bar)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) "
        + "INDEX `BAZ` (`BAR`)";
    sql(sql).ok(expected);
  }

  @Test void testUniqueIndexWithoutName() {
    final String sql = "create table foo (bar integer) unique index (bar)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) "
        + "UNIQUE INDEX (`BAR`)";
    sql(sql).ok(expected);
  }

  @Test void testUniqueIndexWithName() {
    final String sql = "create table foo (bar integer) unique index baz (bar)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER) "
        + "UNIQUE INDEX `BAZ` (`BAR`)";
    sql(sql).ok(expected);
  }

  @Test void testMulticolumnIndex() {
    final String sql = "create table foo (bar integer, qux integer) "
        + "index (bar, qux)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER, `QUX` INTEGER) "
        + "INDEX (`BAR`, `QUX`)";
    sql(sql).ok(expected);
  }

  @Test void testMultipleIndices() {
    final String sql = "create table foo (bar integer, qux integer) "
        + "index (bar), unique index (qux)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER, `QUX` INTEGER) "
        + "INDEX (`BAR`), UNIQUE INDEX (`QUX`)";
    sql(sql).ok(expected);
  }

  @Test void testPrimaryIndexWithSecondaryIndex() {
    final String sql = "create table foo (bar integer, qux integer) "
        + "primary index (bar), index (qux)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER, `QUX` INTEGER) "
        + "PRIMARY INDEX (`BAR`), INDEX (`QUX`)";
    sql(sql).ok(expected);
  }

  @Test void testPrimaryIndexWithSecondaryIndexWithoutComma() {
    final String sql = "create table foo (bar integer, qux integer) "
        + "primary index (bar) index (qux)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER, `QUX` INTEGER) "
        + "PRIMARY INDEX (`BAR`), INDEX (`QUX`)";
    sql(sql).ok(expected);
  }

  @Test void testTopNNoPercentNoTies() {
    final String sql = "select top 5 bar from foo";
    final String expected = "SELECT TOP 5 `BAR`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testTopNWithTies() {
    final String sql = "select top 5 with ties bar from foo";
    final String expected = "SELECT TOP 5 WITH TIES `BAR`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testTopNWithPercent() {
    final String sql = "select top 5.2 percent bar from foo";
    final String expected = "SELECT TOP 5.2 PERCENT `BAR`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testTopNWithPercentWithTies() {
    final String sql = "select top 5 percent with ties bar from foo";
    final String expected = "SELECT TOP 5 PERCENT WITH TIES `BAR`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testTopNGreaterThan100WithoutPercent() {
    final String sql = "select top 500 bar from foo";
    final String expected = "SELECT TOP 500 `BAR`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test void testTopNDecimalWithoutPercentFails() {
    final String sql = "select top ^5.2^ bar from foo";
    final String expected = "(?s).*Cannot specify non-integer value."
        + "*without specifying PERCENT.*";
    sql(sql).fails(expected);
  }

  @Test void testTopNNegativeFails() {
    final String sql = "select ^top^ -5 * from foo";
    final String expected = "(?s).*Encountered \"top -\".*";
    sql(sql).fails(expected);
  }

  @Test void testTopNPercentGreaterThan100Fails() {
    final String sql = "select top 200 ^percent^ bar from foo";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }
}
