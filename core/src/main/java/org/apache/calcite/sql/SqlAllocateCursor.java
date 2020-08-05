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
 * Parse tree for {@code SqlAllocateCursor} call.
 */
public class SqlAllocateCursor extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ALLOCATE CURSOR", SqlKind.ALLOCATE_CURSOR);

  public final SqlIdentifier cursorName;
  public final SqlIdentifier procedureName;

  /**
   * Creates a {@code SqlAllocateCursor}.
   *
   * @param pos  Parser position, must not be null
   * @param cursorName  Name of the cursor previously opened, must not be null
   * @param procedureName  Name of the procedure called, must not be null
   */
  public SqlAllocateCursor(SqlParserPos pos, SqlIdentifier cursorName,
      SqlIdentifier procedureName) {
    super(pos);
    this.cursorName = Objects.requireNonNull(cursorName);
    this.procedureName = Objects.requireNonNull(procedureName);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(cursorName, procedureName);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALLOCATE");
    cursorName.unparse(writer, 0, 0);
    writer.keyword("CURSOR FOR PROCEDURE");
    procedureName.unparse(writer, 0, 0);
  }
}
