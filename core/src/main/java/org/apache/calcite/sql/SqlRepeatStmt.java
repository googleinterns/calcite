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

public class SqlRepeatStmt extends SqlIterationStmt{
  /**
   * Creates a {@code SqlRepeatStmt}.
   *
   * @param pos         Parser position, must not be null.
   * @param condition   Conditional expression, must not be null.
   * @param statements  List of statements to iterate, must not be null.
   * @param beginLabel  Optional begin label, must match end label if not null.
   * @param endLabel    Optional end label, must match begin label if not null.
   */
  public SqlRepeatStmt(final SqlParserPos pos,
      final SqlNode condition, final SqlStatementList statements, final SqlIdentifier beginLabel,
      final SqlIdentifier endLabel) {
    super(pos, condition, statements, beginLabel, endLabel);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    if (label != null) {
      label.unparse(writer, leftPrec, rightPrec);
      writer.print(": ");
    }
    writer.keyword("REPEAT");
    statements.unparse(writer, leftPrec, rightPrec);
    writer.keyword("UNTIL");
    condition.unparse(writer, leftPrec, rightPrec);
    writer.keyword("END REPEAT");
    if (label != null) {
      label.unparse(writer, leftPrec, rightPrec);
    }
  }
}
