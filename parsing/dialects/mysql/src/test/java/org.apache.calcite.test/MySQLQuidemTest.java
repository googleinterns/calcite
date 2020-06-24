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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import net.hydromatic.quidem.Quidem;

import java.sql.Connection;
import java.util.Collection;

class MySQLQuidemTest extends DialectQuidemTest {
  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java MySQLQuidemTest sql/table.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new MySQLQuidemTest().test(arg);
    }
  }

  MySQLQuidemTest() {
    super(MySQLParserImpl.FACTORY);
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/select.iq";
    return data(first);
  }

  @Override Quidem.ConnectionFactory createDialectConnectionFactory() {
    return new QuidemConnectionFactory() {
      @Override public Connection connect(String name, boolean reference)
          throws Exception {
        switch (name) {
        case "scott-mysql":
          return CalciteAssert.that()
              .with(CalciteAssert.Config.SCOTT)
              .with(CalciteConnectionProperty.PARSER_FACTORY,
                  MySQLParserImpl.class.getName() + "#FACTORY")
              .with(CalciteConnectionProperty.CONFORMANCE,
                  SqlConformanceEnum.MYSQL_8)
              .connect();
        default:
          return super.connect(name, reference);
        }
      }
    };
  }
}
