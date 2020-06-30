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
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.redshift.RedshiftParserImpl;

import com.google.common.base.Throwables;

import java.util.Objects;

/**
 * Tests the "Redshift" SQL parser.
 */
final class RedshiftParserTest extends PostgreSQLBaseParserTest {

  @Override protected SqlParserImplFactory parserImplFactory() {
    return RedshiftParserImpl.FACTORY;
  }

  /**
   * Redshift parser's global {@code LOOKAHEAD} is larger than the core
   * parser's. This causes different parse error message between these two
   * parsers. Here we define a looser error checker for Redshift, so that we can
   * reuse failure testing codes from {@link SqlDialectParserTest}.
   *
   * <p>If a test case is written in this file -- that is, not inherited -- it
   * is still checked by {@link SqlDialectParserTest}'s checker.
   */
  @Override protected Tester getTester() {
    return new TesterImpl() {
      @Override protected void checkEx(String expectedMsgPattern,
          SqlParserUtil.StringAndPos sap, Throwable thrown) {
        if (thrownByRedshiftTest(thrown)) {
          super.checkEx(expectedMsgPattern, sap, thrown);
        } else {
          checkExNotNull(sap, thrown);
        }
      }

      private boolean thrownByRedshiftTest(Throwable ex) {
        Throwable rootCause = Throwables.getRootCause(ex);
        StackTraceElement[] stackTrace = rootCause.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
          String className = stackTraceElement.getClassName();
          if (Objects.equals(className, RedshiftParserTest.class.getName())) {
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
}
