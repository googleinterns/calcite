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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code SqlExecMacro} statement.
 */
public class SqlExecMacro extends SqlCall implements SqlExecutableStatement {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("EXECUTE", SqlKind.OTHER);

  private final SqlIdentifier name;
  private final SqlNodeList paramNames;
  private final SqlNodeList paramValues;

  /**
   * Create an {@code SqlExecMacro}.
   *
   * @param pos  Parser position, must not be null
   * @param name  Name of the macro
   * @param paramNames  List of parameter names
   * @param paramValues  List of parameter values
   */
  public SqlExecMacro(SqlParserPos pos, SqlIdentifier name,
      SqlNodeList paramNames, SqlNodeList paramValues) {
    super(pos);
    this.name = Objects.requireNonNull(name);
    this.paramNames = paramNames;
    this.paramValues = paramValues;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    // the list of paramNames could be empty
    return ImmutableNullableList.of(name, paramValues, paramNames);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("EXECUTE");
    name.unparse(writer, leftPrec, rightPrec);
    if (paramValues.size() == 0) {
      return;
    }
    SqlWriter.Frame frame = writer.startList("(", ")");
    if (paramNames.size() == 0) {
      for (SqlNode e : paramValues) {
        writer.sep(",", false);
        e.unparse(writer, 0, 0);
      }
    } else {
      for (int i = 0; i < paramNames.size(); i++) {
        writer.sep(",", false);
        paramNames.get(i).unparse(writer, 0, 0);
        writer.keyword("=");
        paramValues.get(i).unparse(writer, 0, 0);
      }
    }
    writer.endList(frame);

  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
