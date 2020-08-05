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
 * Parse tree for {@code SqlSelectInto} call.
 */
public class SqlSelectInto extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SELECT INTO",
          SqlKind.SELECT_INTO);

  public final SelectIntoWithModifier withModifier;
  public final boolean set;
  public final SqlSelectKeyword selectKeyword;
  public final SqlNodeList selectList;
  public final SqlNodeList parameters;
  public final SqlNode fromClause;
  public final SqlNode whereClause;

  /**
   * Creates a {@code SqlSelectInto}.
   *
   * @param pos  Parser position, must not be null
   * @param withModifier Which recursive modifier was specified, must not be
   *                     null
   * @param set  True if the SET keyword was specified
   * @param selectKeyword Whether DISTINCT or ALL was specified
   * @param selectList  An asterisk or a list of SQL expressions, must not be
   *                    null
   * @param parameters A list of parameter names, must not be null
   * @param fromClause FROM clause
   * @param whereClause WHERE clause
   */
  public SqlSelectInto(SqlParserPos pos, SelectIntoWithModifier withModifier,
      boolean set, SqlSelectKeyword selectKeyword, SqlNodeList selectList,
      SqlNodeList parameters, SqlNode fromClause, SqlNode whereClause) {
    super(pos);
    this.withModifier = Objects.requireNonNull(withModifier);
    this.set = set;
    this.selectKeyword = selectKeyword;
    this.selectList = Objects.requireNonNull(selectList);
    this.parameters = Objects.requireNonNull(parameters);
    this.fromClause = fromClause;
    this.whereClause = whereClause;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(selectList, parameters, fromClause,
        whereClause);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    switch (withModifier) {
    case WITH:
      writer.keyword("WITH");
      break;
    case WITH_RECURSIVE:
      writer.keyword("WITH RECURSIVE");
      break;
    default:
      break;
    }
    writer.keyword("SELECT");
    if (set) {
      writer.keyword("SET");
    }
    if (selectKeyword != null && selectKeyword != SqlSelectKeyword.STREAM) {
      writer.keyword(selectKeyword.toString());
    }
    selectList.unparse(writer, 0, 0);
    writer.setNeedWhitespace(true);
    writer.keyword("INTO");
    parameters.unparse(writer, 0, 0);
    if (fromClause != null) {
      fromClause.unparse(writer, 0, 0);
    }
    if (whereClause != null) {
      whereClause.unparse(writer, 0, 0);
    }
  }

  public enum SelectIntoWithModifier {
    /**
     * Creates a non-recursive query.
     */
    WITH,

    /**
     * Creates a recursive query.
     */
    WITH_RECURSIVE,

    /**
     * With modifier is unspecified.
     */
    UNSPECIFIED,
  }
}
