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
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.dialect1.Dialect1ParserImpl;

import com.google.common.base.Throwables;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the Dialect1 SQL parser.
 */
final class Dialect1ParserTest extends SqlDialectParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return Dialect1ParserImpl.FACTORY;
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

  @Test void testDeleteWithoutFrom() {
    final String sql = "delete t";
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

  /** In Dialect1, AS is not reserved. */
  @Test void testAs() {
    final String expected = "SELECT `AS`\n"
        + "FROM `T`";
    sql("select as from t").ok(expected);
  }

  /** In Dialect1, DESC is not reserved. */
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
   * This is a failure test making sure the LOOKAHEAD for WHEN clause is 2 in
   * Dialect1, where in core parser this number is 1.
   *
   * @see SqlDialectParserTest#testCaseExpression()
   * @see <a href="https://issues.apache.org/jira/browse/CALCITE-2847">[CALCITE-2847]
   * Optimize global LOOKAHEAD for SQL parsers</a>
   */
  @Test void testCaseExpressionDialect1() {
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
   * Dialect1 parser's global {@code LOOKAHEAD} is larger than the core
   * parser's. This causes different parse error message between these two
   * parsers. Here we define a looser error checker for Dialect1, so that we can
   * reuse failure testing codes from {@link SqlDialectParserTest}.
   *
   * <p>If a test case is written in this file -- that is, not inherited -- it
   * is still checked by {@link SqlDialectParserTest}'s checker.
   */
  @Override protected Tester getTester() {
    return new TesterImpl() {
      @Override protected void checkEx(String expectedMsgPattern,
          SqlParserUtil.StringAndPos sap, Throwable thrown) {
        if (thrownByDialect1Test(thrown)) {
          super.checkEx(expectedMsgPattern, sap, thrown);
        } else {
          checkExNotNull(sap, thrown);
        }
      }

      private boolean thrownByDialect1Test(Throwable ex) {
        Throwable rootCause = Throwables.getRootCause(ex);
        StackTraceElement[] stackTrace = rootCause.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
          String className = stackTraceElement.getClassName();
          if (Objects.equals(className, Dialect1ParserTest.class.getName())) {
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

  @Test public void testCompoundIdentifierWithColonSeparator() {
    final String sql = "select * from foo:bar";
    final String expected = "SELECT *\n"
        + "FROM `FOO`.`BAR`";
    sql(sql).ok(expected);
  }

  @Test public void testCompoundIdentifierWithColonAndDotSeparators() {
    final String sql = "select * from foo:bar.baz";
    final String expected = "SELECT *\n"
        + "FROM `FOO`.`BAR`.`BAZ`";
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

  @Test public void testCreateTableOnCommitDeleteRows() {
    final String sql = "create table foo (bar int) on commit delete rows";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER)"
        + " ON COMMIT DELETE ROWS";
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

  @Test public void testCreateTableCompressWithStringLiteralColumnLevelAttribute() {
    final String sql = "create table foo (bar char(3) compress 'xyz')";
    final String expected = "CREATE TABLE `FOO` (`BAR` CHAR(3) COMPRESS 'xyz')";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithNumericLiteralColumnLevelAttribute() {
    final String sql = "create table foo (bar integer compress 3)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS 3)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithNullColumnLevelAttribute() {
    final String sql = "create table foo (bar integer compress (NULL))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS (NULL))";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCompressWithMixedTypesColumnLevelAttribute() {
    final String sql = "create table foo (bar integer compress (1, 'x', "
        + "DATE '1972-02-28'))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER COMPRESS (1, 'x',"
        + " DATE '1972-02-28'))";
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

  @Test public void testCreateTableKanji1CharacterSetColumnLevelAttribute() {
    final String sql = "create table foo (bar int character set kanji1)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET KANJI1)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateTableCharacterSetAndUppercaseColumnLevelAttributes() {
    final String sql = "create table foo (bar int character set kanji1 uppercase)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER CHARACTER SET "
        + "KANJI1 UPPERCASE)";
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

  @Test public void testExecuteMacroWithOneParamValue() {
    final String sql = "execute foo (1)";
    final String expected = "EXECUTE `FOO` (1)";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacroWithOneParamNameWithValue() {
    final String sql = "execute foo (bar = 1)";
    final String expected = "EXECUTE `FOO` (`BAR` = 1)";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacroWithMoreThanOneParamValue() {
    final String sql = "execute foo (1, 'Hello')";
    final String expected = "EXECUTE `FOO` (1, 'Hello')";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacroWithMoreThanOneParamNameWithValue() {
    final String sql = "execute foo (bar = 1.3, "
        + "goo = timestamp '2020-05-30 13:20:00')";
    final String expected = "EXECUTE `FOO` (`BAR` = 1.3, "
        + "`GOO` = TIMESTAMP '2020-05-30 13:20:00')";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacroWithMoreThanOneParamValueWithNull() {
    final String sql = "execute foo (1, null, 3)";
    final String expected = "EXECUTE `FOO` (1, NULL, 3)";
    sql(sql).ok(expected);
  }

  @Test public void testExecuteMacroWithMixedParamPatternFails() {
    final String sql = "execute foo (1, bar ^=^ '2')";
    final String expected = "(?s).*Encountered \"=\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testExecuteMacroWithOmittedValues() {
    final String sql = "execute foo (,,3)";
    final String expected = "EXECUTE `FOO` (NULL, NULL, 3)";
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

  @Test public void testNamedExpressionLiteral() {
    final String sql = "select 1 (named b) from foo";
    final String expected = "SELECT 1 AS `B`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testNamedExpressionSimpleIdentifier() {
    final String sql = "select a (named b) from foo";
    final String expected = "SELECT `A` AS `B`\n"
        + "FROM `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testNamedExpressionCompoundIdentifier() {
    final String sql = "select a.b (named c) from foo";
    final String expected = "SELECT `A`.`B` AS `C`\n"
        + "FROM `FOO`";
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

  @Test public void testInlineModSyntaxSimpleIdentifiers() {
    final String sql = "select foo mod bar";
    final String expected = "SELECT MOD(`FOO`, `BAR`)";
    sql(sql).ok(expected);
  }

  @Test public void testInlineModSyntaxCompoundIdentifiers() {
    final String sql = "select foo.bar mod baz.qux";
    final String expected = "SELECT MOD(`FOO`.`BAR`, `BAZ`.`QUX`)";
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

  @Test public void testTimeFunction() {
    final String sql = "SELECT time-1 FROM foo";
    final String expected = "SELECT (TIME - 1)\n"
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

  @Test public void testMergeIntoSimpleIdentifier() {
    final String sql = "merge into t1 a using t2 b on a.x = b.x when matched then "
        + "update set y = b.y when not matched then insert (x,y) values (b.x, b.y)";
    final String expected = "MERGE INTO `T1` AS `A`\n"
        + "USING `T2` AS `B`\n"
        + "ON (`A`.`X` = `B`.`X`)\n"
        + "WHEN MATCHED THEN UPDATE SET `Y` = `B`.`Y`\n"
        + "WHEN NOT MATCHED THEN INSERT (`X`, `Y`) (VALUES (ROW(`B`.`X`, `B`.`Y`)))";
    sql(sql).ok(expected);
  }

  @Test public void testMergeIntoCompoundIdentifier() {
    final String sql = "merge into t1 a using t2 b on a.x = b.x when matched then "
        + "update set y = b.y when not matched then insert (x.w, y.z) values (b.x.w, b.y.z)";
    final String expected = "MERGE INTO `T1` AS `A`\n"
        + "USING `T2` AS `B`\n"
        + "ON (`A`.`X` = `B`.`X`)\n"
        + "WHEN MATCHED THEN UPDATE SET `Y` = `B`.`Y`\n"
        + "WHEN NOT MATCHED THEN INSERT (`X`.`W`, `Y`.`Z`) "
        + "(VALUES (ROW(`B`.`X`.`W`, `B`.`Y`.`Z`)))";
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
    final String expected = "RENAME TABLE `FOO` AS `BAR`";
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

  @Test public void testRenameMacroWithTo() {
    final String sql = "rename macro foo to bar";
    final String expected = "RENAME MACRO `FOO` AS `BAR`";
    sql(sql).ok(expected);
  }

  @Test public void testRenameMacroWithAs() {
    final String sql = "rename macro foo as bar";
    final String expected = "RENAME MACRO `FOO` AS `BAR`";
    sql(sql).ok(expected);
  }

  @Test public void testRenameMacroWithCompoundIdentifiers() {
    final String sql = "rename macro foo.bar as bar.foo";
    final String expected = "RENAME MACRO `FOO`.`BAR` AS `BAR`.`FOO`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeSimpleIdentifier() {
    final String sql = "select foo (format 'XXX')";
    final String expected = "SELECT CAST(`FOO` AS FORMAT 'XXX')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeCompoundIdentifier() {
    final String sql = "select foo.bar (format 'XXX')";
    final String expected = "SELECT CAST(`FOO`.`BAR` AS FORMAT 'XXX')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeNumericLiteral() {
    final String sql = "select 12.5 (format '9.99E99')";
    final String expected = "SELECT CAST(12.5 AS FORMAT '9.99E99')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeStringLiteral() {
    final String sql = "select 12.5 (format 'XXX')";
    final String expected = "SELECT CAST(12.5 AS FORMAT 'XXX')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeDateLiteral() {
    final String sql = "select current_date (format 'yyyy-mm-dd')";
    final String expected = "SELECT CAST(CURRENT_DATE AS FORMAT 'yyyy-mm-dd')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastFormatAttributeQuery() {
    final String sql = "select (select foo from bar) (format 'XXX') from baz";
    final String expected = "SELECT CAST((SELECT `FOO`\n"
        + "FROM `BAR`) AS FORMAT 'XXX')\n"
        + "FROM `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionSimpleIdentifier() {
    final String sql = "select foo (integer)";
    final String expected = "SELECT CAST(`FOO` AS INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionCompoundIdentifier() {
    final String sql = "select foo.bar (integer)";
    final String expected = "SELECT CAST(`FOO`.`BAR` AS INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionLiteral() {
    final String sql = "select 12.5 (integer)";
    final String expected = "SELECT CAST(12.5 AS INTEGER)";
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
    final String sql = "select (select foo from bar) (time(0), format 'HHhMIm') from baz";
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

  @Test void testAlternativeTypeConversionFormatAttribute() {
    final String sql = "select '15h33m' (time(0), format 'HHhMIm')";
    final String expected = "SELECT CAST('15h33m' AS TIME(0) FORMAT 'HHhMIm')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionTitleAttribute() {
    final String sql = "select foo (char, title 'hello')";
    final String expected = "SELECT CAST(`FOO` AS CHAR TITLE 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionNamedAttribute() {
    final String sql = "select foo (char, named 'hello')";
    final String expected = "SELECT CAST(`FOO` AS CHAR NAMED 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionCharacterSetAttribute() {
    final String sql = "select foo (char, character set latin)";
    final String expected = "SELECT CAST(`FOO` AS CHAR CHARACTER SET LATIN)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionUppercaseAttribute() {
    final String sql = "select foo (char, uppercase)";
    final String expected = "SELECT CAST(`FOO` AS CHAR UPPERCASE)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionAttributesNoDataType() {
    final String sql = "select foo (uppercase, format 'X6', character set "
        + "unicode)";
    final String expected = "SELECT CAST(`FOO` AS UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionAttributesAfterDataType() {
    final String sql = "select foo (char, uppercase, format 'X6', character "
        + "set unicode)";
    final String expected = "SELECT CAST(`FOO` AS CHAR UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionAttributesBeforeDataType() {
    final String sql = "select foo (uppercase, format 'X6', character set "
        + "unicode, char)";
    final String expected = "SELECT CAST(`FOO` AS CHAR UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeTypeConversionAttributesBeforeAndAfterDataType() {
    final String sql = "select foo (uppercase, format 'X6', character set "
        + "unicode, char, title 'hello', named 'hello')";
    final String expected = "SELECT CAST(`FOO` AS CHAR UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE TITLE 'hello' NAMED 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testExplicitCastWithAttributes() {
    final String sql = "select cast(foo as char uppercase format 'X6' "
        + "character set unicode title 'hello' named 'hello')";
    final String expected = "SELECT CAST(`FOO` AS CHAR UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE TITLE 'hello' NAMED 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testExplicitCastWithAttributesNoDataType() {
    final String sql = "select cast(foo as uppercase format 'X6' character "
        + "set unicode title 'hello' named 'hello')";
    final String expected = "SELECT CAST(`FOO` AS UPPERCASE FORMAT 'X6' "
        + "CHARACTER SET UNICODE TITLE 'hello' NAMED 'hello')";
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

  @Test void testAlterSingleTableOption() {
    final String sql = "alter table foo, no fallback";
    final String expected = "ALTER TABLE `FOO`, NO FALLBACK";
    sql(sql).ok(expected);
  }

  @Test void testAlterSingleTableOptionWithValue() {
    final String sql = "alter table foo, freespace = 5";
    final String expected = "ALTER TABLE `FOO`, FREESPACE = 5";
    sql(sql).ok(expected);
  }

  @Test void testAlterMultipleTableOptions() {
    final String sql = "alter table foo, no fallback, no before journal";
    final String expected = "ALTER TABLE `FOO`, NO FALLBACK, NO BEFORE JOURNAL";
    sql(sql).ok(expected);
  }

  @Test void testAlterChecksumWithImmediate() {
    final String sql = "alter table foo, checksum = default immediate";
    final String expected = "ALTER TABLE `FOO`, CHECKSUM = DEFAULT IMMEDIATE";
    sql(sql).ok(expected);
  }

  @Test void testAlterDataBlockSizeWithImmediate() {
    final String sql = "alter table foo, minimum datablocksize immediate";
    final String expected = "ALTER TABLE `FOO`, MINIMUM DATABLOCKSIZE IMMEDIATE";
    sql(sql).ok(expected);
  }

  @Test void testAlterDataBlockSizeWithValueAndImmediate() {
    final String sql = "alter table foo, datablocksize = 5 immediate";
    final String expected = "ALTER TABLE `FOO`, DATABLOCKSIZE = 5 BYTES IMMEDIATE";
    sql(sql).ok(expected);
  }

  @Test void testAlterFreespaceDefault() {
    final String sql = "alter table foo, default freespace";
    final String expected = "ALTER TABLE `FOO`, DEFAULT FREESPACE";
    sql(sql).ok(expected);
  }

  @Test void testAlterOnCommit() {
    final String sql = "alter table foo, on commit delete rows";
    final String expected = "ALTER TABLE `FOO`, ON COMMIT DELETE ROWS";
    sql(sql).ok(expected);
  }

  @Test void testAlterAddColumn() {
    final String sql = "alter table foo add bar integer";
    final String expected = "ALTER TABLE `FOO` ADD (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlterTableCompoundIdentifier() {
    final String sql = "alter table foo.baz add bar integer";
    final String expected = "ALTER TABLE `FOO`.`BAZ` ADD (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlterAddMultiColumns() {
    final String sql = "alter table foo add (bar integer, baz integer)";
    final String expected = "ALTER TABLE `FOO` ADD"
        + " (`BAR` INTEGER, `BAZ` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlterAddColumnWithTableAttribute() {
    final String sql = "alter table foo, no fallback add bar integer";
    final String expected = "ALTER TABLE `FOO`, NO FALLBACK ADD"
        + " (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlterAddColumnEmptyFails() {
    final String sql = "alter table foo ^add^";
    final String expected = "(?s).*Encountered \"add <EOF>\".*";
    sql(sql).fails(expected);
  }

  @Test void testAlterAddColumnEmptyParenthesesFails() {
    final String sql = "alter table foo add ^(^)";
    final String expected = "(?s).*Encountered \"\\( \\)\".*";
    sql(sql).fails(expected);
  }

  @Test void testAlterRename() {
    final String sql = "alter table foo rename bar to baz";
    final String expected = "ALTER TABLE `FOO` RENAME `BAR` TO `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlterRenameWithTableAttribute() {
    final String sql = "alter table foo, no fallback rename bar to baz";
    final String expected = "ALTER TABLE `FOO`, NO FALLBACK"
        + " RENAME `BAR` TO `BAZ`";
    sql(sql).ok(expected);
  }

  @Test void testAlterDrop() {
    final String sql = "alter table foo drop bar";
    final String expected = "ALTER TABLE `FOO` DROP `BAR`";
    sql(sql).ok(expected);
  }

  @Test void testAlterDropWithIdentity() {
    final String sql = "alter table foo drop bar identity";
    final String expected = "ALTER TABLE `FOO` DROP `BAR` IDENTITY";
    sql(sql).ok(expected);
  }

  @Test void testAlterDropWithTableAttribute() {
    final String sql = "alter table foo, no fallback drop bar";
    final String expected = "ALTER TABLE `FOO`, NO FALLBACK DROP `BAR`";
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

  @Test void testColAttributeGeneratedWithAlways() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY)";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithByDefault() {
    final String sql = "create table foo (bar integer"
        + " generated by default as identity)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED BY DEFAULT AS IDENTITY)";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithStartWith() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (start with 5))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (START WITH 5))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithIncrementBy() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (increment by 5))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (INCREMENT BY 5))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedMinValue() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (minvalue 5))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (MINVALUE 5))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithNoMinValue() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no minvalue))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (NO MINVALUE))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithMaxValue() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (maxvalue 5))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (MAXVALUE 5))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithNoMaxValue() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no maxvalue))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (NO MAXVALUE))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithCycle() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (cycle))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (CYCLE))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithNoCycle() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no cycle))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (NO CYCLE))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithMultipleAttributes() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no minvalue no maxvalue))";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER"
        + " GENERATED ALWAYS AS IDENTITY (NO MINVALUE NO MAXVALUE))";
    sql(sql).ok(expected);
  }

  @Test void testColAttributeGeneratedWithNoMinValueAndValueSetFails() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no minvalue ^5^))";
    final String expected = "(?s).*Encountered \"5\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testColAttributeGeneratedWithNoMaxValueAndValueSetFails() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity (no maxvalue ^5^))";
    final String expected = "(?s).*Encountered \"5\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testColAttributeGeneratedWithEmptyParenthesesFails() {
    final String sql = "create table foo (bar integer"
        + " generated always as identity ^(^))";
    final String expected = "(?s).*Encountered \"\\( \\)\" at.*";
    sql(sql).fails(expected);
  }

  @Test public void testInsertOneOmittedValue() {
    final String sql = "insert into foo values (1,,'hi')";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(1, NULL, 'hi'))";
    sql(sql).ok(expected);
  }

  @Test public void testInsertAllOmittedValues() {
    final String sql = "insert into foo values (,,)";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(NULL, NULL, NULL))";
    sql(sql).ok(expected);
  }

  @Test public void testInsertOneOmittedValueNoValuesKeyword() {
    final String sql = "insert into foo (1,,'hi')";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(1, NULL, 'hi'))";
    sql(sql).ok(expected);
  }

  @Test public void testInsertAllOmittedValuesNoValuesKeyword() {
    final String sql = "insert into foo (,,)";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(NULL, NULL, NULL))";
    sql(sql).ok(expected);
  }

  @Test public void testNestedJoin() {
    final String sql = "select * from foo join (bar join baz on bar.a = baz.a)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "INNER JOIN (`BAR` INNER JOIN `BAZ` ON (`BAR`.`A` = `BAZ`.`A`))"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedJoinMultiLevel() {
    final String sql = "select * from foo join (bar join "
        + "(baz join qux on baz.a = qux.a) on bar.a = baz.a)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "INNER JOIN (`BAR` INNER JOIN (`BAZ` INNER JOIN `QUX`"
        + " ON (`BAZ`.`A` = `QUX`.`A`))"
        + " ON (`BAR`.`A` = `BAZ`.`A`))"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedLeftJoin() {
    final String sql = "select * from foo left join"
        + " (bar left join baz on bar.a = baz.a)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "LEFT JOIN (`BAR` LEFT JOIN `BAZ` ON (`BAR`.`A` = `BAZ`.`A`))"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedRightJoin() {
    final String sql = "select * from foo right join"
        + " (bar right join baz on bar.a = baz.a)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "RIGHT JOIN (`BAR` RIGHT JOIN `BAZ` ON (`BAR`.`A` = `BAZ`.`A`))"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedFullJoin() {
    final String sql = "select * from foo full join"
        + " (bar full join baz on bar.a = baz.a)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "FULL JOIN (`BAR` FULL JOIN `BAZ` ON (`BAR`.`A` = `BAZ`.`A`))"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedCrossJoin() {
    final String sql = "select * from foo cross join (bar cross join baz)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "CROSS JOIN (`BAR` CROSS JOIN `BAZ`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedDifferentJoins() {
    final String sql = "select * from foo left join"
        + " (bar cross join baz)"
        + " on foo.a = bar.a";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "LEFT JOIN (`BAR` CROSS JOIN `BAZ`)"
        + " ON (`FOO`.`A` = `BAR`.`A`)";
    sql(sql).ok(expected);
  }

  @Test public void testNestedJoinParenthesizedTableFails() {
    final String sql = "select * from foo cross join (bar cross join (^baz^))";
    final String expected =
        "(?s)Non-query expression encountered in illegal context.*";
    sql(sql).fails(expected);
  }

  @Test public void testNestedJoinParenthesizedUnnestFails() {
    final String sql = "select * from foo cross join"
        + " (bar cross join ^(^unnest(x)))";
    final String expected =
        "(?s)Encountered \"\\( unnest \\( x \\) \\)\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testInlineCaseSpecificAbbreviated() {
    final String sql = "select * from foo where a (not cs) = 'Hello' (cs)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`A` (NOT CASESPECIFIC) = 'Hello' (CASESPECIFIC))";
    sql(sql).ok(expected);
  }

  @Test public void testInlineCaseSpecificNoneEqualsCaseSpecific() {
    final String sql = "select * from foo where a = 'Hello' (casespecific)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`A` = 'Hello' (CASESPECIFIC))";
    sql(sql).ok(expected);
  }

  @Test public void testInlineCaseSpecificNotCaseSpecificEqualsNotCaseSpecific() {
    final String sql = "select * from foo where a (NOT CASESPECIFIC) = 'Hello'"
        + " (not casespecific)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`A` (NOT CASESPECIFIC) = 'Hello' (NOT CASESPECIFIC))";
    sql(sql).ok(expected);
  }

  @Test public void testInlineCaseSpecificNotCaseSpecificEqualsCaseSpecific() {
    final String sql = "select * from foo where a (NOT CASESPECIFIC) = 'Hello' (casespecific)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`A` (NOT CASESPECIFIC) = 'Hello' (CASESPECIFIC))";
    sql(sql).ok(expected);
  }

  @Test public void testInlineCaseSpecificFunctionCall() {
    final String sql = "select * from foo where MY_FUN(a) (CASESPECIFIC) = 'Hello'";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`MY_FUN`(`A`) (CASESPECIFIC) = 'Hello')";
    sql(sql).ok(expected);
  }

  @Test public void testInlineCaseSpecificCompoundIdentifier() {
    final String sql = "select * from foo as f where f.a (casespecific) = 'Hello'";
    final String expected = "SELECT *\n"
        + "FROM `FOO` AS `F`\n"
        + "WHERE (`F`.`A` (CASESPECIFIC) = 'Hello')";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableExecPositionalParams() {
    final String sql = "exec foo (:bar, :baz, :qux)";
    final String expected = "EXECUTE `FOO` (:BAR, :BAZ, :QUX)";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableExecNamedParams() {
    final String sql = "exec foo (bar=:bar, baz=:baz, qux=:qux)";
    final String expected = "EXECUTE `FOO` (`BAR` = :BAR, `BAZ` = :BAZ,"
        + " `QUX` = :QUX)";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableSelect() {
    final String sql = "select :bar as baz from foo where a = :qux";
    final String expected = "SELECT :BAR AS `BAZ`\n"
        + "FROM `FOO`\n"
        + "WHERE (`A` = :QUX)";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableInsert() {
    final String sql = "insert into foo values (:bar, :baz)";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(:BAR, :BAZ))";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableInsertWithoutValuesKeyword() {
    final String sql = "insert into foo (:bar, :baz)";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(:BAR, :BAZ))";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableUpdate() {
    final String sql = "update foo set bar = :baz";
    final String expected = "UPDATE `FOO` SET `BAR` = :BAZ";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableCast() {
    final String sql = "select cast(:foo as bar)";
    final String expected = "SELECT CAST(:FOO AS `BAR`)";
    sql(sql).ok(expected);
  }

  @Test public void testHostVariableNonReservedKeywords() {
    final String sql = "insert into foo values (:a, :avg)";
    final String expected = "INSERT INTO `FOO`\n"
        + "VALUES (ROW(:A, :AVG))";
    sql(sql).ok(expected);
  }

  @Test public void testCastByteInt() {
    final String sql = "select cast(x as byteint)";
    final String expected = "SELECT CAST(`X` AS BYTEINT)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonType() {
    final String sql = "create table foo (x json)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeMaxLength() {
    final String sql = "create table foo (x json(33))";
    final String expected = "CREATE TABLE `FOO` (`X` JSON(33))";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeInlineLength() {
    final String sql = "create table foo (x json inline length 33)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON INLINE LENGTH 33)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeMaxLengthAndInlineLength() {
    final String sql = "create table foo (x json(33) inline length 20)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON(33) INLINE LENGTH 20)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeCharacterSetLatin() {
    final String sql = "create table foo (x json character set latin)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON CHARACTER SET LATIN)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeCharacterSetUnicode() {
    final String sql = "create table foo (x json character set unicode)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON CHARACTER SET UNICODE)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeStorageFormatBson() {
    final String sql = "create table foo (x json storage format bson)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON STORAGE FORMAT BSON)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeStorageFormatUbjson() {
    final String sql = "create table foo (x json storage format ubjson)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON STORAGE FORMAT UBJSON)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeFormatAttribute() {
    final String sql = "create table foo (x json format 'XXX')";
    final String expected = "CREATE TABLE `FOO` (`X` JSON FORMAT 'XXX')";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeDefaultNullAttribute() {
    final String sql = "create table foo (x json default null)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON DEFAULT NULL)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeNullAttribute() {
    final String sql = "create table foo (x json null)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeNotNullAttribute() {
    final String sql = "create table foo (x json not null)";
    final String expected = "CREATE TABLE `FOO` (`X` JSON NOT NULL)";
    sql(sql).ok(expected);
  }

  @Test public void testJsonTypeMaxLengthOneFails() {
    final String sql = "create table foo (x json(^1^))";
    final String expected = "Numeric literal '1' out of range";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeMaxLengthZeroFails() {
    final String sql = "create table foo (x json(^0^))";
    final String expected = "Numeric literal '0' out of range";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeMaxLengthNegativeFails() {
    final String sql = "create table foo (x json^(^-1))";
    final String expected = "(?s).*Encountered \"\\( -\".*";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeInlineLengthLargerThanMaxLengthFails() {
    final String sql = "create table foo (x json(3) inline length ^4^)";
    final String expected = "Numeric literal '4' out of range";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeInLineLengthZeroFails() {
    final String sql = "create table foo (x json inline length ^0^)";
    final String expected = "Numeric literal '0' out of range";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeInLineLengthNegativeFails() {
    final String sql = "create table foo (x json inline length ^-1)";
    final String expected = "(?s).*Encountered \"-\".*";
    sql(sql).fails(expected);
  }

  @Test public void testJsonTypeCharacterSetAndStorageFormatSpecifiedFails() {
    final String sql = "create table foo (x json character set latin storage format ^bson^)";
    final String expected = "Query expression encountered in illegal context";
    sql(sql).fails(expected);
  }

  @Test void testHexCharLiteralCharSetNotSpecifiedDefaultFormat() {
    final String sql = "'c1a'XC";
    final String expected = "'c1a' XC";
    expr(sql).ok(expected);
  }

  @Test void testHexCharLiteralCharSetSpecifiedXCFormat() {
    final String sql = "_KANJISJIS 'ABC'XC";
    final String expected = "_KANJISJIS 'ABC' XC";
    expr(sql).ok(expected);
  }

  @Test void testHexCharLiteralCharSetKanji1Format() {
    final String sql = "_KANJI1 'ABC'XC";
    final String expected = "_KANJI1 'ABC' XC";
    expr(sql).ok(expected);
  }

  @Test void testHexCharLiteralCharSetSpecifiedXCVFormat() {
    final String sql = "_LATIN'c1a'XCV";
    final String expected = "_LATIN 'c1a' XCV";
    expr(sql).ok(expected);
  }

  @Test void testHexCharLiteralCharSetSpecifiedXCFFormat() {
    final String sql = "_unicode'c1a'XCF";
    final String expected = "_UNICODE 'c1a' XCF";
    expr(sql).ok(expected);
  }

  @Test void testHexCharLiteralOutsideRangeFails() {
    // 'g' contains char outside hex range, it would be incorrectly parsed
    // falling into the <PREFIXED_STRING_LITERAL> which leads to errors
    final String sql = "^_unicode'cg'^XCF";
    final String expected = "Unknown character set 'unicode'";
    expr(sql).fails(expected);
  }

  @Test void testHexCharLiteralInQuery() {
    final String sql = "select _LATIN'c1A'XCV";
    final String expected = "SELECT _LATIN 'c1A' XCV";
    sql(sql).ok(expected);
  }

  @Test public void testVarbyte() {
    final String sql = "create table foo (bar varbyte(20))";
    final String expected = "CREATE TABLE `FOO` (`BAR` VARBYTE(20))";
    sql(sql).ok(expected);
  }

  @Test public void testVarbyteMaxValue() {
    final String sql = "create table foo (bar varbyte(64000))";
    final String expected = "CREATE TABLE `FOO` (`BAR` VARBYTE(64000))";
    sql(sql).ok(expected);
  }

  @Test public void testVarbyteCast() {
    final String sql = "select cast(foo as varbyte(100))";
    final String expected = "SELECT CAST(`FOO` AS VARBYTE(100))";
    sql(sql).ok(expected);
  }

  @Test public void testVarbyteOutOfRangeFails() {
    final String sql = "create table foo (bar varbyte(^64001^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testVarbyteNegativeFails() {
    final String sql = "create table foo (bar varbyte(^-^1))";
    final String expected = "(?s).*Encountered \"-\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testByte() {
    final String sql = "create table foo (bar byte)";
    final String expected = "CREATE TABLE `FOO` (`BAR` BYTE)";
    sql(sql).ok(expected);
  }

  @Test public void testByteWithValue() {
    final String sql = "create table foo (bar byte(20))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BYTE(20))";
    sql(sql).ok(expected);
  }

  @Test public void testByteMaxValue() {
    final String sql = "create table foo (bar byte(64000))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BYTE(64000))";
    sql(sql).ok(expected);
  }

  @Test public void testByteCast() {
    final String sql = "select cast(foo as byte(100))";
    final String expected = "SELECT CAST(`FOO` AS BYTE(100))";
    sql(sql).ok(expected);
  }

  @Test public void testByteOutOfRangeFails() {
    final String sql = "create table foo (bar byte(^64001^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testByteNegativeFails() {
    final String sql = "create table foo (bar byte^(^-1))";
    final String expected = "(?s).*Encountered \"\\( -\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testLikeAnySingleOption() {
    final String sql = "select * from foo where bar like any ('a')";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` LIKE SOME ('a'))";
    sql(sql).ok(expected);
  }

  @Test public void testNotLikeAny() {
    final String sql = "select * from foo where bar not like any ('a')";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` NOT LIKE SOME ('a'))";
    sql(sql).ok(expected);
  }

  @Test public void testLikeSubquery() {
    final String sql = "select * from foo where bar like any"
        + " (select * from baz)";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` LIKE SOME (SELECT *\n"
        + "FROM `BAZ`))";
    sql(sql).ok(expected);
  }

  @Test public void testLikeAnyMultipleOptions() {
    final String sql = "select * from foo where bar like any ('a', 'b')";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` LIKE SOME ('a', 'b'))";
    sql(sql).ok(expected);
  }

  @Test public void testLikeSomeMultipleOptions() {
    final String sql = "select * from foo where bar like some ('a', 'b')";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` LIKE SOME ('a', 'b'))";
    sql(sql).ok(expected);
  }

  @Test public void testLikeAllMultipleOptions() {
    final String sql = "select * from foo where bar like all ('a', 'b')";
    final String expected = "SELECT *\n"
        + "FROM `FOO`\n"
        + "WHERE (`BAR` LIKE ALL ('a', 'b'))";
    sql(sql).ok(expected);
  }

  @Override @Test public void testSome() {
    final String sql = "select * from emp\n"
        + "where sal > some (select comm from emp)";
    final String expected = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`SAL` > SOME (SELECT `COMM`\n"
        + "FROM `EMP`))";
    sql(sql).ok(expected);

    // ANY is a synonym for SOME
    final String sql2 = "select * from emp\n"
        + "where sal > any (select comm from emp)";
    sql(sql2).ok(expected);

    final String sql3 = "select * from emp\n"
        + "where name like (select ^some^ name from emp)";
    sql(sql3).fails("(?s).*Encountered \"some name\" at .*");

    final String sql4 = "select * from emp\n"
        + "where name like some (select name from emp)";
    final String expected4 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`NAME` LIKE SOME (SELECT `NAME`\n"
        +  "FROM `EMP`))";
    sql(sql4).ok(expected4);

    final String sql5 = "select * from emp where empno = any (10,20)";
    final String expected5 = "SELECT *\n"
        + "FROM `EMP`\n"
        + "WHERE (`EMPNO` = SOME (10, 20))";
    sql(sql5).ok(expected5);
  }

  @Test public void testPeriodTypeNameSpecDate() {
    final String sql = "create table foo (a period(date))";
    final String expected = "CREATE TABLE `FOO` (`A` PERIOD(DATE))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTime() {
    final String sql = "create table foo (a period(time))";
    final String expected = "CREATE TABLE `FOO` (`A` PERIOD(TIME))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTimeWithPrecision() {
    final String sql = "create table foo (a period(time(2)))";
    final String expected = "CREATE TABLE `FOO` (`A` PERIOD(TIME(2)))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTimeWithTimezone() {
    final String sql = "create table foo (a period(time with time zone))";
    final String expected =
        "CREATE TABLE `FOO` (`A` PERIOD(TIME WITH TIME ZONE))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTimeWithPrecisionWithTimezone() {
    final String sql = "create table foo (a period(time(2) with time zone))";
    final String expected =
        "CREATE TABLE `FOO` (`A` PERIOD(TIME(2) WITH TIME ZONE))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTimeStamp() {
    final String sql = "create table foo (a period(timestamp))";
    final String expected = "CREATE TABLE `FOO` (`A` PERIOD(TIMESTAMP))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecTimeStampWithPrecision() {
    final String sql = "create table foo (a period(timestamp(0)))";
    final String expected = "CREATE TABLE `FOO` (`A` PERIOD(TIMESTAMP(0)))";
    sql(sql).ok(expected);
  }

  @Test public void testPeriodTypeNameSpecDateWithPrecisionFails() {
    final String sql = "create table foo (a period(date^(^0)))";
    final String expected = "(?s).*Encountered \"\\(\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testPeriodTypeNameSpecDateWithTimezoneFails() {
    final String sql = "create table foo (a period(date ^with^ time zone))";
    final String expected = "(?s).*Encountered \"with\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testPeriodTypeNameSpecPrecisionOutOfRangeFails() {
    final String sql = "create table foo (a period(timestamp(7)^)^)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testPeriodTypeNameSpecNegativePrecisionFails() {
    final String sql = "create table foo (a period(time^(^-1)))";
    final String expected = "(?s).*Encountered \"\\( -\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testNumberDataType() {
    final String sql = "create table foo (bar number)";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER)";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypePrecision() {
    final String sql = "create table foo (bar number(3))";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER(3))";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypePrecisionStar() {
    final String sql = "create table foo (bar number(*))";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER(*))";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypePrecisionScale() {
    final String sql = "create table foo (bar number(*, 3))";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER(*, 3))";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypePrecisionScaleMaxValues() {
    final String sql = "create table foo (bar number(38, 38))";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER(38, 38))";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypePrecisionScaleMinValues() {
    final String sql = "create table foo (bar number(1, 0))";
    final String expected = "CREATE TABLE `FOO` (`BAR` NUMBER(1, 0))";
    sql(sql).ok(expected);
  }

  @Test public void testNumberDataTypeMinPrecisionOutOfRangeFails() {
    final String sql = "create table foo (bar number(^0^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testNumberDataTypeMaxPrecisionOutOfRangeFails() {
    final String sql = "create table foo (bar number(^39^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testNumberDataTypeScaleGreaterThanPrecisionFails() {
    final String sql = "create table foo (bar number(12, ^13^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test public void testNumberDataTypeMinScaleOutOfRangeFails() {
    final String sql = "create table foo (bar number(*^,^ -1))";
    final String expected = "(?s).*Encountered \", -\" at .*";
    sql(sql).fails(expected);
  }

  @Test public void testNumberDataTypeMaxScaleOutOfRangeFails() {
    final String sql = "create table foo (bar number(*, ^39^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testAlternativeTypeConversionWithNamedFunction() {
    final String sql = "SELECT foo(a) (INT)";
    final String expected = "SELECT CAST(`FOO`(`A`) AS INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testAlternativeCastAttributeNamedFunction() {
    final String sql = "select foo(a) (format 'X6')";
    final String expected = "SELECT CAST(`FOO`(`A`) AS FORMAT 'X6')";
    sql(sql).ok(expected);
  }

  @Test void testNamedExpressionWithNamedFunction() {
    final String sql = "select foo(a) (named b)";
    final String expected = "SELECT `FOO`(`A`) AS `B`";
    sql(sql).ok(expected);
  }

  @Test void testBlob() {
    final String sql = "create table foo (bar blob)";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB)";
    sql(sql).ok(expected);
  }

  @Test void testBinaryLargeObject() {
    final String sql = "create table foo (bar binary large object)";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB)";
    sql(sql).ok(expected);
  }

  @Test void testBlobValue() {
    final String sql = "create table foo (bar blob(1000))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB(1000))";
    sql(sql).ok(expected);
  }

  @Test void testBlobValueKilobytes() {
    final String sql = "create table foo (bar blob(2047937k))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB(2047937K))";
    sql(sql).ok(expected);
  }

  @Test void testBlobValueMegabytes() {
    final String sql = "create table foo (bar blob(1999m))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB(1999M))";
    sql(sql).ok(expected);
  }

  @Test void testBlobValueGigabytes() {
    final String sql = "create table foo (bar blob(1g))";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB(1G))";
    sql(sql).ok(expected);
  }

  @Test void testBlobValueWithAttributes() {
    final String sql = "create table foo (bar blob(1g) not null format 'x(4)' "
        + "title 'hello')";
    final String expected = "CREATE TABLE `FOO` (`BAR` BLOB(1G) NOT NULL FORMAT 'x(4)' "
        + "TITLE 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testBlobValueCast() {
    final String sql = "select cast(foo as blob(10000) format 'x(6)')";
    final String expected = "SELECT CAST(`FOO` AS BLOB(10000) FORMAT 'x(6)')";
    sql(sql).ok(expected);
  }

  @Test void testBlobOutOfRangeFails() {
    final String sql = "create table foo (bar blob(^2097088001^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testBlobKilobytesOutOfRangeFails() {
    final String sql = "create table foo (bar blob(^2047938^k))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testBlobMegabytesOutOfRangeFails() {
    final String sql = "create table foo (bar blob(^2000^m))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testBlobGigabytesOutOfRangeFails() {
    final String sql = "create table foo (bar blob(^2^g))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testBlobZeroFails() {
    final String sql = "create table foo (bar blob(^0^))";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClob() {
    final String sql = "create table foo (bar clob)";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB)";
    sql(sql).ok(expected);
  }

  @Test void testCharacterLargeObject() {
    final String sql = "create table foo (bar character large object)";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB)";
    sql(sql).ok(expected);
  }

  @Test void testCharacterLargeObjectValue() {
    final String sql = "create table foo (bar character large object(50))";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(50))";
    sql(sql).ok(expected);
  }

  @Test void testClobValue() {
    final String sql = "create table foo (bar clob(1000))";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(1000))";
    sql(sql).ok(expected);
  }

  @Test void testClobValueKilobytes() {
    final String sql = "create table foo (bar clob(2047937k))";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(2047937K))";
    sql(sql).ok(expected);
  }

  @Test void testClobValueMegabytes() {
    final String sql = "create table foo (bar clob(1999m))";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(1999M))";
    sql(sql).ok(expected);
  }

  @Test void testClobValueGigabytes() {
    final String sql = "create table foo (bar clob(1g))";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(1G))";
    sql(sql).ok(expected);
  }

  @Test void testClobValueCharacterSetLatin() {
    final String sql = "create table foo (bar clob(100) character set latin)";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(100) CHARACTER SET "
        + "LATIN)";
    sql(sql).ok(expected);
  }

  @Test void testClobValueCharacterSetUnicode() {
    final String sql = "create table foo (bar clob(100) character set unicode)";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(100) CHARACTER SET "
        + "UNICODE)";
    sql(sql).ok(expected);
  }

  @Test void testClobValueWithCharacterSetAndAttributes() {
    final String sql = "create table foo (bar clob(1g) character set latin not "
        + "null format 'x(4)' "
        + "title 'hello')";
    final String expected = "CREATE TABLE `FOO` (`BAR` CLOB(1G) CHARACTER SET "
        + "LATIN NOT NULL FORMAT 'x(4)' "
        + "TITLE 'hello')";
    sql(sql).ok(expected);
  }

  @Test void testClobLatinOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^2097088001^) character set "
        + "latin)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobUnicodeOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^1048544001^) character set "
        + "unicode)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobKilobytesLatinOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^2047938^k) character set "
        + "latin)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobKilobytesUnicodeOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^1023969^k) character set "
        + "unicode)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobMegabytesLatinOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^2000^m) character set "
        + "latin)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobMegabytesUnicodeOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^1000^m) character set "
        + "unicode)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobGigabytesLatinOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^2^g) character set latin)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobGigabytesUnicodeOutOfRangeFails() {
    final String sql = "create table foo (bar clob(^1^g) character set "
        + "unicode)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testClobZeroFails() {
    final String sql = "create table foo (bar clob(^0^) character set "
        + "unicode)";
    final String expected = "(?s).*Numeric literal.*out of range.*";
    sql(sql).fails(expected);
  }

  @Test void testCreateTableAlias() {
    final String sql = "ct foo (bar integer)";
    final String expected = "CREATE TABLE `FOO` (`BAR` INTEGER)";
    sql(sql).ok(expected);
  }

  @Test void testCreateTableAliasTableFails() {
    final String sql = "^ct^ table foo (bar integer)";
    final String expected = "(?s).*Encountered \"ct table\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testCreateTableAliasOrReplaceFails() {
    final String sql = "ct or ^replace^ foo (bar integer)";
    final String expected = "(?s).*Encountered \"replace\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testCreateTableAliasMultisetFails() {
    final String sql = "ct multiset ^foo^ (bar integer)";
    final String expected = "(?s).*Encountered \"foo\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testCreateTableAliasSetFails() {
    final String sql = "^ct^ set foo (bar integer)";
    final String expected = "(?s).*Encountered \"ct set\" at.*";
    sql(sql).fails(expected);
  }

  @Test void testCreateTableAliasVolatileFails() {
    final String sql = "^ct^ volatile foo (bar integer)";
    final String expected = "(?s).*Encountered \"ct volatile\" at.*";
    sql(sql).fails(expected);
  }

  @Test public void testDropMacro() {
    final String sql = "drop macro foo";
    final String expected = "DROP MACRO `FOO`";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMacroNoAttributes() {
    final String sql = "create macro foo as (select * from bar;)";
    final String expected = "CREATE MACRO `FOO` AS (SELECT *\n"
        + "FROM `BAR`;)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMacroInsertStatement() {
    final String sql = "create macro foo (num int) as (insert into bar (num)"
        + " values (:num);)";
    final String expected = "CREATE MACRO `FOO` (`NUM` INTEGER) AS "
        + "(INSERT INTO `BAR` (`NUM`)\n"
        + "VALUES (ROW(:NUM));)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMacroInsertAndSelectStatements() {
    final String sql = "create macro foo (dob date format 'mmddyy') as "
        + "(insert into bar (dob) values(:dob);"
        + " select * from bar where dob = :dob; )";
    final String expected = "CREATE MACRO `FOO` (`DOB` DATE FORMAT 'mmddyy') AS"
        + " (INSERT INTO `BAR` (`DOB`)\n"
        + "VALUES (ROW(:DOB)); SELECT *\n"
        + "FROM `BAR`\n"
        + "WHERE (`DOB` = :DOB);)";
    sql(sql).ok(expected);
  }

  @Test public void testCreateMacroUpdateAndSelectStatements() {
    final String sql = "create macro foo (num int default 99, val varchar"
        + " not null) as (update bar set num = :num where val = :val;"
        + " select * from bar where num = :num)";
    final String expected = "CREATE MACRO `FOO` (`NUM` INTEGER DEFAULT 99, `VAL`"
        + " VARCHAR NOT NULL) AS (UPDATE `BAR` SET `NUM` = :NUM\n"
        + "WHERE (`VAL` = :VAL); SELECT *\n"
        + "FROM `BAR`\n"
        + "WHERE (`NUM` = :NUM);)";
    sql(sql).ok(expected);
  }
}
