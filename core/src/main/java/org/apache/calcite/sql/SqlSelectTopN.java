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
 * {@code SqlSelectTopN} is a class that handles the TOP n syntax in
 * SELECT statements. It is used by the {@link SqlSelect} class.
 */
public class SqlSelectTopN extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("TOP", SqlKind.TOP_N);
  private final SqlNumericLiteral selectNum;
  private final SqlLiteral isPercent;
  private final SqlLiteral withTies;

  public SqlSelectTopN(SqlParserPos pos, SqlNumericLiteral selectNum,
      SqlLiteral isPercent, SqlLiteral withTies) {
    super(pos);
    this.selectNum = selectNum;
    this.isPercent = isPercent;
    this.withTies = withTies;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(selectNum, isPercent, withTies);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("TOP");
    selectNum.unparse(writer, leftPrec, rightPrec);
    if (isPercent != null && isPercent.getValueAs(Boolean.class)) {
      writer.keyword("PERCENT");
    }
    if (withTies != null && withTies.getValueAs(Boolean.class)) {
      writer.keyword("WITH TIES");
    }
  }
}
