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

import java.util.List;
import java.util.Objects;

/**
 * Parse tree representing a conditional expression pairing with a list of
 * statements.
 */
public class SqlConditionalStmtListPair extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CONDITION_STATEMENT_LIST_PAIR", SqlKind.OTHER);

  public final SqlNode condition;
  public final SqlStatementList stmtList;

  /**
   * Creates a {@code SqlConditionalStmtListPair}.
   * @param pos         Parser position, must not be null.
   * @param condition   Condition expression, must not be null.
   * @param stmtList    A List of statements, must not be null.
   */
  public SqlConditionalStmtListPair(final SqlParserPos pos,
      final SqlNode condition,
      final SqlStatementList stmtList) {
    super(pos);
    this.condition = Objects.requireNonNull(condition);
    this.stmtList = Objects.requireNonNull(stmtList);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(condition, stmtList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    condition.unparse(writer, leftPrec, rightPrec);
    writer.keyword("THEN");
    stmtList.unparse(writer, leftPrec, rightPrec);
  }
}
