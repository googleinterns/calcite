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
 * Parse tree for Partition By Column expression.
 */
public class SqlTablePartitionByColumn extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PARTITION_BY_COLUMN", SqlKind.OTHER);
  public final SqlNodeList columnItemList;
  public final boolean containAllButSpecifier;

  /**
   * Creates a {@code SqlTablePartitionByColumn}.
   * @param pos             Parser position, must not be null.
   * @param columnItemList  Partition expressions in a SqlNodeList.
   * @param containAllButSpecifier
   *                        If "ALL BUT" token is specified.
   */
  public SqlTablePartitionByColumn(final SqlParserPos pos,
      final SqlNodeList columnItemList, final boolean containAllButSpecifier) {
    super(pos);
    this.columnItemList = columnItemList;
    this.containAllButSpecifier = containAllButSpecifier;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(columnItemList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("COLUMN");
    if (columnItemList.size() == 0) {
      return;
    }
    if (containAllButSpecifier) {
      writer.keyword("ALL BUT");
    }
    SqlWriter.Frame frame = writer.startList(
        SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlNode columnItem : columnItemList) {
      writer.sep(",");
      columnItem.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}
