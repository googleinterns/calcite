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
package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code SqlBeginRequestCall} expression.
 */
public class SqlBeginRequestCall extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("BEGIN_REQUEST", SqlKind.BEGIN_REQUEST);

  public final SqlStatementList statements;

  /**
   * Creates an instance of {@code SqlBeginRequestCall}.
   *
   * @param pos SQL parser position
   * @param statements List of SQL statements
   */
  public SqlBeginRequestCall(SqlParserPos pos, SqlStatementList statements) {
    super(pos);
    this.statements = statements;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("BEGIN REQUEST");
    writer.newlineAndIndent();
    statements.unparse(writer, 0, 0);
    writer.keyword("END REQUEST");
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(statements);
  }
}
