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

/**
 * Parse tree for {@code SqlDeclareVariable} expression.
 */
public class SqlDeclareVariable extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DECLARE_VARIABLE",
          SqlKind.DECLARE_VARIABLE);

  public final SqlNodeList variableNames;
  public final SqlDataTypeSpec dataType;
  public final SqlNode defaultValue;

  /**
   * Creates an instance of {@code SqlDeclareVariable}.
   *
   * @param pos SQL parser position
   * @param variableNames List of variable names to declare
   * @param dataType The data type of the variables
   * @param defaultValue The default value, may be null
   */
  public SqlDeclareVariable(SqlParserPos pos, SqlNodeList variableNames,
      SqlDataTypeSpec dataType, SqlNode defaultValue) {
    super(pos);
    this.variableNames = variableNames;
    this.dataType = dataType;
    this.defaultValue = defaultValue;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DECLARE");
    variableNames.unparse(writer, 0, 0);
    dataType.unparse(writer, 0, 0);
    if (defaultValue != null) {
      writer.keyword("DEFAULT");
      defaultValue.unparse(writer, 0, 0);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(variableNames, dataType, defaultValue);
  }
}
