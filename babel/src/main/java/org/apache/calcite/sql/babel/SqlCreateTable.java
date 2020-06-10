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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
public class SqlCreateTable extends SqlCreate
    implements SqlExecutableStatement {
  public final SqlIdentifier name;
  public final SetType setType;
  public final Volatility volatility;
  public final List<SqlCreateAttribute> tableAttributes;
  public final SqlNodeList columnList;
  public final SqlNode query;
  public final boolean withData;
  public final OnCommitType onCommitType;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

  /** Creates a SqlCreateTable. */
  public SqlCreateTable(SqlParserPos pos, boolean replace, SetType setType, Volatility volatility,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    this(pos, replace, setType, volatility, ifNotExists, name, /*tableAttributes=*/null,
        columnList, query, /*withData=*/false, /*onCommitType=*/OnCommitType.UNSPECIFIED);
  }

  public SqlCreateTable(SqlParserPos pos, boolean replace, SetType setType, Volatility volatility,
      boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query,
      boolean withData, OnCommitType onCommitType) {
    this(pos, replace, setType, volatility, ifNotExists, name, /*tableAttributes=*/null,
        columnList, query, withData, onCommitType);
  }

  public SqlCreateTable(SqlParserPos pos, boolean replace, SetType setType, Volatility volatility,
      boolean ifNotExists, SqlIdentifier name, List<SqlCreateAttribute> tableAttributes,
      SqlNodeList columnList, SqlNode query,
      boolean withData, OnCommitType onCommitType) {
    super(OPERATOR, pos, replace, ifNotExists);
    this.name = Objects.requireNonNull(name);
    this.setType = setType;
    this.volatility = volatility;
    this.tableAttributes = tableAttributes; // may be null
    this.columnList = columnList; // may be null
    this.query = query; // for "CREATE TABLE ... AS query"; may be null
    this.withData = withData;
    this.onCommitType = onCommitType;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
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
      for (SqlCreateAttribute a : tableAttributes) {
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
    if (withData) {
      writer.keyword("WITH DATA");
    }
    switch (onCommitType) {
    case PRESERVE:
      writer.keyword("ON COMMIT PRESERVE ROWS");
      break;
    case RELEASE:
      writer.keyword("ON COMMIT RELEASE ROWS");
      break;
    default:
      break;
    }
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
