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
 * Parse tree for a {@code SqlIfStmt}.
 */
public class SqlIfStmt extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("IF", SqlKind.CONDITIONAL_STATEMENT);

  public final SqlNodeList conditionalStmtListPairs;
  public final SqlNodeList elseStmtList;

  /**
   * Creates a {@code SqlIfStmt}.
   * @param pos                       Parser position, must not be null.
   * @param conditionalStmtListPairs  List of conditional expression pairs
   *                                  with StatementList, must not be null.
   * @param elseStmtList              List of statements in the else clause,
   *                                  must not be null.
   */
  public SqlIfStmt(final SqlParserPos pos,
      final SqlNodeList conditionalStmtListPairs,
      final SqlNodeList elseStmtList) {
    super(pos);
    this.conditionalStmtListPairs =
        Objects.requireNonNull(conditionalStmtListPairs);
    this.elseStmtList = Objects.requireNonNull(elseStmtList);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(conditionalStmtListPairs, elseStmtList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    for (int i = 0; i < conditionalStmtListPairs.size(); i++) {
      if (i != 0) {
        writer.keyword("ELSE IF");
      } else {
        writer.keyword("IF");
      }
      conditionalStmtListPairs.get(i).unparse(writer, leftPrec, rightPrec);
    }
    if (!SqlNodeList.isEmptyList(elseStmtList)) {
      writer.keyword("ELSE");
      elseStmtList.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("END IF");
  }
}
