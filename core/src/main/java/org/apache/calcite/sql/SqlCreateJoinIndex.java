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
 * Parse tree for {@code CREATE JOIN INDEX} statement.
 */
public class SqlCreateJoinIndex extends SqlCreate {
  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE JOIN INDEX", SqlKind.CREATE_TABLE);

  public final SqlIdentifier name;
  public final List<SqlTableAttribute> tableAttributes;
  public final SqlNodeList select;
  public final SqlNode from;
  public final SqlNode where;
  public final SqlNodeList groupBy;
  public final SqlNodeList orderBy;
  public final List<SqlIndex> indices;

  /** Creates a {@code SqlCreateJoinIndex}. */
  public SqlCreateJoinIndex(SqlParserPos pos, SqlIdentifier name,
      List<SqlTableAttribute> tableAttributes, SqlNodeList select, SqlNode from,
      SqlNode where, SqlNodeList groupBy, SqlNodeList orderBy,
      List<SqlIndex> indices) {
    super(OPERATOR, pos, SqlCreateSpecifier.CREATE, false);
    this.name = name;
    this.tableAttributes = tableAttributes;
    this.select = select;
    this.from = from;
    this.where = where;
    this.groupBy = groupBy;
    this.orderBy = orderBy;
    this.indices = indices;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, select, from, where,
        groupBy, orderBy);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE JOIN INDEX");
    name.unparse(writer, 0, 0);
    if (tableAttributes != null) {
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlTableAttribute a : tableAttributes) {
        writer.sep(",", true);
        a.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS SELECT");
    select.unparse(writer, 0, 0);
    writer.keyword("FROM");
    final SqlWriter.Frame fromFrame =
        writer.startList(SqlWriter.FrameTypeEnum.FROM_LIST);
    from.unparse(
        writer,
        SqlJoin.OPERATOR.getLeftPrec() - 1,
        SqlJoin.OPERATOR.getRightPrec() - 1);
    writer.endList(fromFrame);
    if (where != null) {
      writer.keyword("WHERE");
      where.unparse(writer, 0, 0);
    }
    if (groupBy != null) {
      writer.keyword("GROUP BY");
      groupBy.unparse(writer, 0, 0);
    }
    if (orderBy != null) {
      writer.keyword("ORDER BY");
      orderBy.unparse(writer, 0, 0);
    }
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
