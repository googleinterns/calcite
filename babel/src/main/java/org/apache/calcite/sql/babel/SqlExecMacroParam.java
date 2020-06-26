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
package org.apache.calcite.sql.babel;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code SqlExecMacroParam} SqlCall.
 */
public class SqlExecMacroParam extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PARAM EQUAL", SqlKind.OTHER);

  private final SqlNode name;
  private final SqlNode value;

  /**
   * Create an {@code SqlExecMacroParam} without name.
   *
   * @param pos  Parser position, must not be null
   * @param value  Value of the parameter
   */
  public SqlExecMacroParam(SqlParserPos pos, SqlNode value) {
    super(pos);
    this.name = null;
    this.value = value;
  }

  /**
   * Create an {@code SqlExecMacroParam}.
   *
   * @param pos  Parser position, must not be null
   * @param name  Name of the parameter
   * @param value  Value of the parameter
   */
  public SqlExecMacroParam(SqlParserPos pos, SqlNode name, SqlNode value) {
    super(pos);
    this.name = name;
    this.value = value;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    if (this.name == null) {
      return ImmutableNullableList.of(value);
    } else {
      return ImmutableNullableList.of(name, value);
    }
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    if (this.name == null) {
      value.unparse(writer, 0, 0);
      return;
    }
    name.unparse(writer, 0, 0);
    writer.keyword("=");
    value.unparse(writer, 0, 0);
  }
}
