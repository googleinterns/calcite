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

/**
 * Parse tree for a {@code SqlLoopStmt}.
 */
public class SqlLoopStmt extends SqlIterationStmt {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("LOOP", SqlKind.LOOP_STATEMENT);

  /**
   * Creates a {@code SqlLoopStmt}.
   * @param pos         Parser position, must not be null.
   * @param statements  List of statements to iterate, must not be null.
   * @param beginLabel  Optional begin label, must match end label if not null.
   * @param endLabel    Optional end label, must match begin label if not null.
   */
  public SqlLoopStmt(final SqlParserPos pos, final SqlStatementList statements,
      final SqlIdentifier beginLabel, final SqlIdentifier endLabel) {
    super(pos, /*condition = */null, statements, beginLabel, endLabel);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    unparseBeginLabel(writer, leftPrec, rightPrec);
    writer.keyword("LOOP");
    statements.unparse(writer, leftPrec, rightPrec);
    writer.keyword("END LOOP");
    unparseEndLabel(writer, leftPrec, rightPrec);
  }
}
