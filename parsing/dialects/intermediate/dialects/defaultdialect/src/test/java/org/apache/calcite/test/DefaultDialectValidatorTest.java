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
import org.apache.calcite.sql.parser.defaultdialect.DefaultDialectParserImpl;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

public class DefaultDialectValidatorTest extends SqlValidatorTest {

  @Override public SqlTester getTester() {
    return new SqlValidatorTester(
        SqlTestFactory.INSTANCE
            .with("parserFactory", DefaultDialectParserImpl.FACTORY));
  }

  /** Tests matching of built-in operator names. */
  @Test void testUnquotedBuiltInFunctionNames() {
    // TODO: Once Oracle is an officially supported dialect, this test
    // should be moved to OracleValidatorTest.java.
    final Sql oracle = sql("?")
        .withUnquotedCasing(Casing.TO_UPPER)
        .withCaseSensitive(true);

    // Built-in functions are always case-insensitive.
    oracle.sql("select count(*), sum(deptno), floor(2.5) from dept").ok();
    oracle.sql("select COUNT(*), FLOOR(2.5) from dept").ok();
    oracle.sql("select cOuNt(*), FlOOr(2.5) from dept").ok();
    oracle.sql("select cOuNt (*), FlOOr (2.5) from dept").ok();
    oracle.sql("select current_time from dept").ok();
    oracle.sql("select Current_Time from dept").ok();
    oracle.sql("select CURRENT_TIME from dept").ok();

    oracle.sql("select \"count\"(*) from dept").ok();
  }

  @Test void testDeleteBindExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = ?").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = ?").ok();
  }

  @Test void testTimestampAddAndDiff() {
    List<String> tsi = ImmutableList.<String>builder()
        .add("FRAC_SECOND")
        .add("MICROSECOND")
        .add("MINUTE")
        .add("HOUR")
        .add("DAY")
        .add("WEEK")
        .add("MONTH")
        .add("QUARTER")
        .add("YEAR")
        .add("SQL_TSI_FRAC_SECOND")
        .add("SQL_TSI_MICROSECOND")
        .add("SQL_TSI_MINUTE")
        .add("SQL_TSI_HOUR")
        .add("SQL_TSI_DAY")
        .add("SQL_TSI_WEEK")
        .add("SQL_TSI_MONTH")
        .add("SQL_TSI_QUARTER")
        .add("SQL_TSI_YEAR")
        .build();

    List<String> functions = ImmutableList.<String>builder()
        .add("timestampadd(%s, 12, current_timestamp)")
        .add("timestampdiff(%s, current_timestamp, current_timestamp)")
        .build();

    for (String interval : tsi) {
      for (String function : functions) {
        expr(String.format(Locale.ROOT, function, interval)).ok();
      }
    }

    expr("timestampadd(SQL_TSI_WEEK, 2, current_timestamp)")
        .columnType("TIMESTAMP(0) NOT NULL");
    expr("timestampadd(SQL_TSI_WEEK, 2, cast(null as timestamp))")
        .columnType("TIMESTAMP(0)");
    expr("timestampdiff(SQL_TSI_WEEK, current_timestamp, current_timestamp)")
        .columnType("INTEGER NOT NULL");
    expr("timestampdiff(SQL_TSI_WEEK, cast(null as timestamp), current_timestamp)")
        .columnType("INTEGER");

    wholeExpr("timestampadd(incorrect, 1, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
    wholeExpr("timestampdiff(incorrect, current_timestamp, current_timestamp)")
        .fails("(?s).*Was expecting one of.*");
  }

  @Test void testTimestampAddNullInterval() {
    expr("timestampadd(SQL_TSI_SECOND, cast(NULL AS INTEGER),"
        + " current_timestamp)")
        .columnType("TIMESTAMP(0)");
    expr("timestampadd(SQL_TSI_DAY, cast(NULL AS INTEGER),"
        + " current_timestamp)")
        .columnType("TIMESTAMP(0)");
  }

  @Test void testArithmeticOperatorsFails() {
    expr("^power(2,'abc')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER.<INTEGER>, <CHAR.3.>.*");
    expr("power(2,'abc')")
        .columnType("DOUBLE NOT NULL");
    expr("^power(true,1)^")
        .fails("(?s).*Cannot apply 'POWER' to arguments of type "
            + "'POWER.<BOOLEAN>, <INTEGER>.*");
    expr("^mod(x'1100',1)^")
        .fails("(?s).*Cannot apply 'MOD' to arguments of type "
            + "'MOD.<BINARY.2.>, <INTEGER>.*");
    expr("^mod(1, x'1100')^")
        .fails("(?s).*Cannot apply 'MOD' to arguments of type "
            + "'MOD.<INTEGER>, <BINARY.2.>.*");
    expr("^abs(x'')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'ABS' to arguments of type 'ABS.<BINARY.0.>.*");
    expr("^ln(x'face12')^")
        .fails("(?s).*Cannot apply 'LN' to arguments of type 'LN.<BINARY.3.>.*");
    expr("^log10(x'fa')^")
        .fails("(?s).*Cannot apply 'LOG10' to arguments of type 'LOG10.<BINARY.1.>.*");
    expr("^exp('abc')^")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply 'EXP' to arguments of type 'EXP.<CHAR.3.>.*");
    expr("exp('abc')")
        .columnType("DOUBLE NOT NULL");

    expr("^CURRENT_DATE+1^")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<DATE> \\+ <INTEGER>'.*");
    expr("^1+CURRENT_DATE^")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<INTEGER> \\+ <DATE>'.*");
    expr("^CURRENT_DATE-1^")
        .fails("(?s).*Cannot apply '-' to arguments of type "
            + "'<DATE> - <INTEGER>'.*");
    expr("^-1+CURRENT_DATE^")
        .fails("(?s).*Cannot apply '\\+' to arguments of type "
            + "'<INTEGER> \\+ <DATE>'.*");
  }

  @Test void testMinusDateOperator() {
    expr("(CURRENT_DATE - CURRENT_DATE) HOUR")
        .columnType("INTERVAL HOUR NOT NULL");
    expr("(CURRENT_DATE - CURRENT_DATE) YEAR TO MONTH")
        .columnType("INTERVAL YEAR TO MONTH NOT NULL");
    wholeExpr("(CURRENT_DATE - LOCALTIME) YEAR TO MONTH")
        .fails("(?s).*Parameters must be of the same type.*");
  }

  @Test void testDeleteExtendedColumnFailDuplicate() {
    final Sql s = sql("?").withExtendedCatalog();
    sql("delete from emp (extra VARCHAR, ^extra^ VARCHAR)")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^extra^ VARCHAR)"
        + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
    s.sql("delete from EMP_MODIFIABLEVIEW (extra VARCHAR, ^\"EXTRA\"^ VARCHAR)"
        + " where extra = 'test'")
        .fails("Duplicate name 'EXTRA' in column list");
  }

  @Test void testDeleteExtendedColumn() {
    sql("delete from empdefaults(extra BOOLEAN) where deptno = 10").ok();
    sql("delete from empdefaults(extra BOOLEAN) where extra = false").ok();
  }

  @Test void testDateTime() {
    // LOCAL_TIME
    expr("LOCALTIME(3)").ok();
    expr("LOCALTIME").ok(); // fix sqlcontext later.
    wholeExpr("LOCALTIME(1+2)")
        .fails("Argument to function 'LOCALTIME' must be a literal");
    wholeExpr("LOCALTIME(NULL)")
        .withTypeCoercion(false)
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME(NULL)")
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME(CAST(NULL AS INTEGER))")
        .fails("Argument to function 'LOCALTIME' must not be NULL");
    wholeExpr("LOCALTIME()")
        .fails("No match found for function signature LOCALTIME..");
    //  with TZ?
    expr("LOCALTIME")
        .columnType("TIME(0) NOT NULL");
    wholeExpr("LOCALTIME(-1)")
        .fails("Argument to function 'LOCALTIME' must be a positive integer literal");
    expr("^LOCALTIME(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("LOCALTIME(4)")
        .fails("Argument to function 'LOCALTIME' must be a valid precision "
            + "between '0' and '3'");
    wholeExpr("LOCALTIME('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("LOCALTIME('foo')")
        .fails("Argument to function 'LOCALTIME' must be a literal");

    // LOCALTIMESTAMP
    expr("LOCALTIMESTAMP(3)").ok();
    //    fix sqlcontext later.
    expr("LOCALTIMESTAMP").ok();
    wholeExpr("LOCALTIMESTAMP(1+2)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a literal");
    wholeExpr("LOCALTIMESTAMP()")
        .fails("No match found for function signature LOCALTIMESTAMP..");
    // with TZ?
    expr("LOCALTIMESTAMP")
        .columnType("TIMESTAMP(0) NOT NULL");
    wholeExpr("LOCALTIMESTAMP(-1)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a positive "
            + "integer literal");
    expr("^LOCALTIMESTAMP(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("LOCALTIMESTAMP(4)")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a valid "
            + "precision between '0' and '3'");
    wholeExpr("LOCALTIMESTAMP('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("LOCALTIMESTAMP('foo')")
        .fails("Argument to function 'LOCALTIMESTAMP' must be a literal");

    // CURRENT_DATE
    wholeExpr("CURRENT_DATE(3)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    //    fix sqlcontext later.
    expr("CURRENT_DATE").ok();
    wholeExpr("CURRENT_DATE(1+2)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    wholeExpr("CURRENT_DATE()")
        .fails("No match found for function signature CURRENT_DATE..");
    //  with TZ?
    expr("CURRENT_DATE")
        .columnType("DATE NOT NULL");
    // I guess -s1 is an expression?
    wholeExpr("CURRENT_DATE(-1)")
        .fails("No match found for function signature CURRENT_DATE..NUMERIC..");
    wholeExpr("CURRENT_DATE('foo')")
        .fails(ANY);

    // current_time
    expr("current_time(3)").ok();
    //    fix sqlcontext later.
    expr("current_time").ok();
    wholeExpr("current_time(1+2)")
        .fails("Argument to function 'CURRENT_TIME' must be a literal");
    wholeExpr("current_time()")
        .fails("No match found for function signature CURRENT_TIME..");
    // with TZ?
    expr("current_time")
        .columnType("TIME(0) NOT NULL");
    wholeExpr("current_time(-1)")
        .fails("Argument to function 'CURRENT_TIME' must be a positive integer literal");
    expr("^CURRENT_TIME(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("CURRENT_TIME(4)")
        .fails("Argument to function 'CURRENT_TIME' must be a valid precision "
            + "between '0' and '3'");
    wholeExpr("current_time('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("current_time('foo')")
        .fails("Argument to function 'CURRENT_TIME' must be a literal");

    // current_timestamp
    expr("CURRENT_TIMESTAMP(3)").ok();
    //    fix sqlcontext later.
    expr("CURRENT_TIMESTAMP").ok();
    sql("SELECT CURRENT_TIMESTAMP AS X FROM (VALUES (1))").ok();
    wholeExpr("CURRENT_TIMESTAMP(1+2)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a literal");
    wholeExpr("CURRENT_TIMESTAMP()")
        .fails("No match found for function signature CURRENT_TIMESTAMP..");
    // should type be 'TIMESTAMP with TZ'?
    expr("CURRENT_TIMESTAMP")
        .columnType("TIMESTAMP(0) NOT NULL");
    // should type be 'TIMESTAMP with TZ'?
    expr("CURRENT_TIMESTAMP(2)")
        .columnType("TIMESTAMP(2) NOT NULL");
    wholeExpr("CURRENT_TIMESTAMP(-1)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a positive "
            + "integer literal");
    expr("^CURRENT_TIMESTAMP(100000000000000)^")
        .fails("(?s).*Numeric literal '100000000000000' out of range.*");
    wholeExpr("CURRENT_TIMESTAMP(4)")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a valid "
            + "precision between '0' and '3'");
    wholeExpr("CURRENT_TIMESTAMP('foo')")
        .withTypeCoercion(false)
        .fails("(?s).*Cannot apply.*");
    wholeExpr("CURRENT_TIMESTAMP('foo')")
        .fails("Argument to function 'CURRENT_TIMESTAMP' must be a literal");

    // Date literals
    expr("DATE '2004-12-01'").ok();
    expr("TIME '12:01:01'").ok();
    expr("TIME '11:59:59.99'").ok();
    expr("TIME '12:01:01.001'").ok();
    expr("TIMESTAMP '2004-12-01 12:01:01'").ok();
    expr("TIMESTAMP '2004-12-01 12:01:01.001'").ok();

    // REVIEW: Can't think of any date/time/ts literals that will parse,
    // but not validate.
  }
}
