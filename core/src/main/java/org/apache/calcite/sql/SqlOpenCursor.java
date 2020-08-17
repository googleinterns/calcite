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
 * Parse tree for {@code SqlOpenCursor} call.
 */
public class SqlOpenCursor extends SqlScriptingNode {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("OPEN_CURSOR", SqlKind.OPEN_CURSOR);

  public final SqlIdentifier cursorName;
  public final SqlNodeList parameters;

  /**
   * Creates a {@code SqlOpenCursor}.
   *
   * @param pos  Parser position, must not be null
   * @param cursorName  Name of the cursor to open, must not be null
   * @param parameters  List of variables used as input to SQL statement, must
   *                    not be null
   */
  public SqlOpenCursor(SqlParserPos pos, SqlIdentifier cursorName,
      SqlNodeList parameters) {
    super(pos);
    this.cursorName = Objects.requireNonNull(cursorName);
    this.parameters = Objects.requireNonNull(parameters);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(cursorName, parameters);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("OPEN");
    cursorName.unparse(writer, 0, 0);
    if (!SqlNodeList.isEmptyList(parameters)) {
      writer.keyword("USING");
      parameters.unparse(writer, 0, 0);
    }
  }
}
