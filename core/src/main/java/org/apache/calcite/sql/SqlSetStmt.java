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
 * Parse tree for a {@code SqlSetStmt}.
 */
public class SqlSetStmt extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SET", SqlKind.SET_STATEMENT);

  public final SqlIdentifier target;
  public final SqlNode source;

  /**
   * Creates a {@code SqlSetStmt}.
   *
   * @param pos     Parser position, must not be null.
   * @param target  The name of the variable or parameter to be assigned a
   *                value, cannot be null.
   * @param source  The value to be assigned to target, cannot be null.
   */
  public SqlSetStmt(final SqlParserPos pos, final SqlIdentifier target,
      final SqlNode source) {
    super(pos);
    this.target = Objects.requireNonNull(target);
    this.source = Objects.requireNonNull(source);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(target, source);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("SET");
    target.unparse(writer, leftPrec, rightPrec);
    writer.keyword("=");
    source.unparse(writer, leftPrec, rightPrec);
  }
}
