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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTableDialect1 extends SqlCreateTable
    implements SqlExecutableStatement {
  public final SetType setType;
  public final Volatility volatility;
  public final List<SqlTableAttribute> tableAttributes;
  public final WithDataType withData;
  public final SqlPrimaryIndex primaryIndex;
  public final List<SqlIndex> indices;
  public final OnCommitType onCommitType;

  public SqlCreateTableDialect1(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SetType setType, Volatility volatility, boolean ifNotExists,
      SqlIdentifier name, List<SqlTableAttribute> tableAttributes,
      SqlNodeList columnList, SqlNode query, WithDataType withData,
      SqlPrimaryIndex primaryIndex, OnCommitType onCommitType) {
    this(pos, createSpecifier, setType, volatility, ifNotExists,
        name, tableAttributes, columnList, query, withData,
        primaryIndex, /*indices=*/ null, onCommitType);
  }

  public SqlCreateTableDialect1(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SetType setType, Volatility volatility, boolean ifNotExists,
      SqlIdentifier name, List<SqlTableAttribute> tableAttributes,
      SqlNodeList columnList, SqlNode query, WithDataType withData,
      SqlPrimaryIndex primaryIndex, List<SqlIndex> indices,
      OnCommitType onCommitType) {
    super(pos, createSpecifier, ifNotExists, name, columnList, query);
    this.setType = setType;
    this.volatility = volatility;
    this.tableAttributes = tableAttributes; // may be null
    this.withData = withData;
    this.primaryIndex = primaryIndex;
    this.indices = indices;
    this.onCommitType = onCommitType;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getCreateSpecifier().toString());
    switch (setType) {
    case SET:
      writer.keyword("SET");
      break;
    case MULTISET:
      writer.keyword("MULTISET");
      break;
    default:
      break;
    }
    switch (volatility) {
    case VOLATILE:
      writer.keyword("VOLATILE");
      break;
    case TEMP:
      writer.keyword("TEMP");
      break;
    default:
      break;
    }
    writer.keyword("TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
    if (tableAttributes != null) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlTableAttribute a : tableAttributes) {
        writer.sep(",", true);
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    if (query != null) {
      writer.keyword("AS");
      writer.newlineAndIndent();
      query.unparse(writer, 0, 0);
    }
    switch (withData) {
    case WITH_DATA:
      writer.keyword("WITH DATA");
      break;
    case WITH_NO_DATA:
      writer.keyword("WITH NO DATA");
      break;
    default:
      break;
    }
    List<SqlIndex> allIndices = new ArrayList<>();
    if (primaryIndex != null) {
      allIndices.add(0, primaryIndex);
    }
    if (indices != null) {
      allIndices.addAll(indices);
    }
    if (!allIndices.isEmpty()) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlIndex index : allIndices) {
        writer.sep(",");
        index.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    switch (onCommitType) {
    case PRESERVE:
      writer.keyword("ON COMMIT PRESERVE ROWS");
      break;
    case DELETE:
      writer.keyword("ON COMMIT DELETE ROWS");
      break;
    default:
      break;
    }
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
