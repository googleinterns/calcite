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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code USING} statement.
 */
public class SqlUsingRequestModifier extends SqlCall implements SqlExecutableStatement {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("USING", SqlKind.OTHER);

  private final SqlNodeList columnList;

  /** Creates a SqlUsingRequestModifier */
  public SqlUsingRequestModifier(SqlParserPos pos, SqlNodeList columnList) {
    super(pos);
    this.columnList = Objects.requireNonNull(columnList);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(columnList);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("USING");
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode c : columnList) {
      writer.sep(",");
      c.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
