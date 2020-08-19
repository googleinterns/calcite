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
 * Parse tree for {@code SqlGetDiagnosticsParam} call.
 */
public class SqlGetDiagnosticsParam extends SqlScriptingNode {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("GET DIAGNOSTICS PARAM",
          SqlKind.OTHER);

  public final SqlIdentifier name;
  public final SqlIdentifier value;

  /**
   * Creates a {@code SqlGetDiagnosticsParam}.
   *
   * @param pos Parser position, must not be null
   * @param name Local variable or parameter, must not be null
   * @param value Field from Condition Area, must not be null
   */
  public SqlGetDiagnosticsParam(SqlParserPos pos, SqlIdentifier name,
      SqlIdentifier value) {
    super(pos);
    this.name = Objects.requireNonNull(name);
    this.value = Objects.requireNonNull(value);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, value);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    writer.keyword("=");
    value.unparse(writer, 0, 0);
  }
}
