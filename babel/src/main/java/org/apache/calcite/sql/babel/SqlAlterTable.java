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

import org.apache.calcite.sql.SqlAlter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code ALTER TABLE} statement.
 */
public class SqlAlterTable extends SqlAlter {
  final SqlIdentifier tableName;
  final List<SqlTableAttribute> tableAttributes;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

  public SqlAlterTable(SqlParserPos pos, String scope, SqlIdentifier tableName,
      List<SqlTableAttribute> tableAttributes) {
    super(pos, scope);
    this.tableName = tableName;
    this.tableAttributes = tableAttributes;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER TABLE");
    unparseAlterOperation(writer, leftPrec, rightPrec);
  }

  @Override protected void unparseAlterOperation(SqlWriter writer,
      int leftPrec, int rightPrec) {
    tableName.unparse(writer, leftPrec, rightPrec);
    if (tableAttributes != null) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlTableAttribute a : tableAttributes) {
        writer.sep(",", true);
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName);
  }
}
