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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
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

  @Test public void testSelRewrite() {
    String sql = "sel a from abc";
    String expected = "SELECT `ABC`.`A`\n"
        + "FROM `ABC` AS `ABC`";
    sql(sql).rewritesTo(expected);
  }

  @Test public void testCaretNegation() throws SqlParseException {
    String sql = "select * from dept where ^deptno = 1";
    SqlParser.Config config = configBuilder()
        .setParserFactory(Dialect1ParserImpl.FACTORY)
        .build();
    SqlParser sqlParserReader = SqlParser.create(new StringReader(sql), config);
    SqlNode node = sqlParserReader.parseQuery();
    SqlValidator validator = tester.getValidator();
    validator.validate(node);
  }
}
