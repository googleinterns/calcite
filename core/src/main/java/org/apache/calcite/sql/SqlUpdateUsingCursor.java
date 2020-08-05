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
 * Parse tree for {@code SqlUpdateUsingCursor} call.
 */
public class SqlUpdateUsingCursor extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATE_USING_CURSOR", SqlKind.UPDATE_USING_CURSOR);

  public final SqlIdentifier tableName;
  public final SqlIdentifier aliasName;
  public final SqlNodeList assignments;
  public final SqlIdentifier cursorName;

  /**
   * Creates a {@code SqlUpdateUsingCursor}.
   *
   * @param pos Parser position, must not be null
   * @param tableName Name of table to be updated, must not be null
   * @param aliasName Alias for the table name
   * @param assignments List of "column=value" expressions, must not be null
   * @param cursorName Name of cursor pointing to rows to update, must not be
   *                   null
   */
  public SqlUpdateUsingCursor(SqlParserPos pos, SqlIdentifier tableName,
      SqlIdentifier aliasName, SqlNodeList assignments, SqlIdentifier cursorName) {
    super(pos);
    this.tableName = Objects.requireNonNull(tableName);
    this.aliasName = aliasName;
    this.assignments = Objects.requireNonNull(assignments);
    this.cursorName = Objects.requireNonNull(cursorName);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName, aliasName, assignments,
        cursorName);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("UPDATE");
    tableName.unparse(writer, 0, 0);
    if (aliasName != null) {
      aliasName.unparse(writer, 0, 0);
    }
    writer.keyword("SET");
    assignments.unparse(writer, 0, 0);
    writer.keyword("WHERE CURRENT OF");
    cursorName.unparse(writer, 0, 0);
  }
}
