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
 * Parse tree for {@code SqlSelectAndConsume} call.
 */
public class SqlSelectAndConsume extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SELECT AND CONSUME",
          SqlKind.SELECT_AND_CONSUME);

  public final boolean set;
  public final SqlNodeList selectList;
  public final SqlNodeList parameters;
  public final SqlIdentifier fromTable;

  /**
   * Creates a {@code SqlSelectAndConsume}.
   *
   * @param pos  Parser position, must not be null
   * @param set  True if the SET keyword was specified
   * @param selectList  An asterisk or a list of SQL expressions, must not be
   *                    null
   * @param parameters A list of parameter names
   * @param fromTable Name of the queue table
   */
  public SqlSelectAndConsume(SqlParserPos pos, boolean set,
      SqlNodeList selectList, SqlNodeList parameters,
      SqlIdentifier fromTable) {
    super(pos);
    this.set = set;
    this.selectList = Objects.requireNonNull(selectList);
    this.parameters = Objects.requireNonNull(parameters);
    this.fromTable = Objects.requireNonNull(fromTable);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(selectList, parameters, fromTable);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SELECT");
    if (set) {
      writer.keyword("SET");
    }
    writer.keyword("AND CONSUME TOP 1");
    selectList.unparse(writer, 0, 0);
    writer.setNeedWhitespace(true);
    writer.keyword("INTO");
    parameters.unparse(writer, 0, 0);
    writer.keyword("FROM");
    fromTable.unparse(writer, 0, 0);
  }
}
