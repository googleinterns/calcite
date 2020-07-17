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
 * Parse tree for {@code CREATE MACRO} statement.
 */
public class SqlCreateMacro extends SqlCreate implements SqlExecutableStatement {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CREATE MACRO", SqlKind.CREATE_MACRO);

  public final SqlIdentifier macroName;
  public final SqlNodeList attributes;
  public final SqlNodeList sqlStatements;

  /** Creates a {@code SqlCreateMacro}. */
  public SqlCreateMacro(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SqlIdentifier macroName, SqlNodeList attributes,
      SqlNodeList sqlStatements) {
    super(OPERATOR, pos, createSpecifier, false);
    this.macroName = Objects.requireNonNull(macroName);
    this.attributes = attributes; // May be null.
    this.sqlStatements = Objects.requireNonNull(sqlStatements);
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(attributes, sqlStatements);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getCreateSpecifier().toString());
    writer.keyword("MACRO");
    macroName.unparse(writer, leftPrec, rightPrec);
    if (attributes != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode a : attributes) {
        writer.sep(",");
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS");

    // Custom FrameType.CREATE_MACRO required to avoid SELECT statements
    // being treated as sub queries and being unparsed with parentheses.
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.CREATE_MACRO, "(", ")");
    for (SqlNode s : sqlStatements) {
      s.unparse(writer, 0, 0);
      writer.setNeedWhitespace(false);
      writer.sep(";");
    }
    writer.endList(frame);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
