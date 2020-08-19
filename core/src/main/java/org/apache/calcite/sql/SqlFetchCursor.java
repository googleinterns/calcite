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
 * Parse tree for {@code SqlFetchCursor} call.
 */
public class SqlFetchCursor extends SqlScriptingNode {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("FETCH_CURSOR", SqlKind.FETCH_CURSOR);

  public final FetchType fetchType;
  public final SqlIdentifier cursorName;
  public final SqlNodeList parameters;

  /**
   * Creates an instance of {@code SqlFetchCursor}.
   *
   * @param pos Parser position, must not be null
   * @param fetchType Specifies which row will be fetched, must not be null
   * @param cursorName Name of the cursor to fetch from, must not be null
   * @param parameters Name of the OUT/INOUT parameters to assign values from
   *                   fetched row, must not be null
   */
  public SqlFetchCursor(SqlParserPos pos, FetchType fetchType,
      SqlIdentifier cursorName, SqlNodeList parameters) {
    super(pos);
    this.fetchType = Objects.requireNonNull(fetchType);
    this.cursorName = Objects.requireNonNull(cursorName);
    this.parameters = Objects.requireNonNull(parameters);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(cursorName, parameters);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("FETCH");
    if (fetchType != FetchType.UNSPECIFIED) {
      writer.keyword(fetchType + " FROM");
    }
    cursorName.unparse(writer, 0, 0);
    writer.keyword("INTO");
    parameters.unparse(writer, 0, 0);
  }

  public enum FetchType {
    /**
     * Fetches the next row.
     */
    NEXT,

    /**
     * Fetches the first row.
     */
    FIRST,

    /**
     * Fetch type is not specified.
     */
    UNSPECIFIED,
  }
}
