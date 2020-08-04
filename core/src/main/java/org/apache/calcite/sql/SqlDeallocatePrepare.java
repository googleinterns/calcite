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
 * Parse tree for {@code SqlDeallocatePrepare} call.
 */
public class SqlDeallocatePrepare extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DEALLOCATE PREPARE",
          SqlKind.DEALLOCATE_PREPARE);

  public final SqlIdentifier statementName;

  /**
   * Creates a {@code SqlDeallocatePrepare}.
   *
   * @param pos  Parser position, must not be null
   * @param statementName  Name of the statement to deallocate, must not be null
   */
  public SqlDeallocatePrepare(SqlParserPos pos, SqlIdentifier statementName) {
    super(pos);
    this.statementName = Objects.requireNonNull(statementName);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(statementName);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DEALLOCATE PREPARE");
    statementName.unparse(writer, 0, 0);
  }
}
