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
 * Parse tree for a {@code SqlForStmt}.
 */
public class SqlForStmt extends SqlIterationStmt {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("FOR", SqlKind.FOR_STATEMENT);

  public final SqlIdentifier forLoopVariable;
  public final SqlIdentifier cursorName;
  public final SqlNode cursorSpecification;

  /**
   * Creates a {@code SqlForStmt}.
   * @param pos         Parser position, must not be null.
   * @param statements  List of statements to iterate, must not be null.
   * @param beginLabel  Optional begin label, must match end label if not null.
   * @param endLabel    Optional end label, must match begin label if not null.
   * @param forLoopVariable
   *                    The name of the loop.
   * @param cursorName  The name of the cursor.
   * @param cursorSpecification
   *                    A single select statement used as the cursor.
   */
  public SqlForStmt(final SqlParserPos pos,
      final SqlStatementList statements, final SqlIdentifier beginLabel,
      final SqlIdentifier endLabel, final SqlIdentifier forLoopVariable,
      final SqlIdentifier cursorName, final SqlNode cursorSpecification) {
    super(pos, /*condition = */null, statements, beginLabel, endLabel);
    this.forLoopVariable = Objects.requireNonNull(forLoopVariable);
    this.cursorName = cursorName;
    this.cursorSpecification = Objects.requireNonNull(cursorSpecification);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(statements, label, forLoopVariable,
        cursorName, cursorSpecification);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    unparseBeginLabel(writer, leftPrec, rightPrec);
    writer.keyword("FOR");
    forLoopVariable.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    if (cursorName != null) {
      cursorName.unparse(writer, leftPrec, rightPrec);
      writer.keyword("CURSOR FOR");
    }
    cursorSpecification.unparse(writer, leftPrec, rightPrec);
    writer.keyword("DO");
    statements.unparse(writer, leftPrec, rightPrec);
    writer.keyword("END FOR");
    unparseEndLabel(writer, leftPrec, rightPrec);
  }
}
