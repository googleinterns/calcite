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
 * Parse tree for {@code CREATE JOIN INDEX} statement.
 */
public class SqlCreateJoinIndex extends SqlCreate {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE JOIN INDEX",
          SqlKind.CREATE_JOIN_INDEX);

  public final SqlIdentifier name;
  public final List<SqlTableAttribute> tableAttributes;
  public final SqlNode select;
  public final List<SqlIndex> indices;

  /** Creates a {@code SqlCreateJoinIndex}. */
  public SqlCreateJoinIndex(SqlParserPos pos, SqlIdentifier name,
      List<SqlTableAttribute> tableAttributes, SqlNode select,
      List<SqlIndex> indices) {
    super(OPERATOR, pos, SqlCreateSpecifier.CREATE, false);
    this.name = Objects.requireNonNull(name);
    this.tableAttributes = Objects.requireNonNull(tableAttributes);
    this.select = Objects.requireNonNull(select);
    this.indices = Objects.requireNonNull(indices);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, select);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE JOIN INDEX");
    name.unparse(writer, 0, 0);
    if (!tableAttributes.isEmpty()) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlTableAttribute a : tableAttributes) {
        writer.sep(",", true);
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS");
    select.unparse(writer, 0, 0);
    if (!indices.isEmpty()) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlIndex index : indices) {
        writer.sep(",");
        index.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }
}
