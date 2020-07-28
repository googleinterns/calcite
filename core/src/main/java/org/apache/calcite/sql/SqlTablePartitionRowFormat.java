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
 * Parse tree for Row Format Partition expression.
 */
public class SqlTablePartitionRowFormat extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ROW_FORMAT_PARTITION", SqlKind.OTHER);
  public final SqlNodeList columnList;
  public final CompressionOpt compressionOpt;

  /**
   * Creates a {@code SqlTablePartitionRowFormat}.
   * @param pos             Parser position, must not be null.
   * @param columnList      List of columns in a SqlNodeList.
   * @param compressionOpt  Option about if the auto compression is enabled.
   */
  public SqlTablePartitionRowFormat(final SqlParserPos pos,
      final SqlNodeList columnList, final CompressionOpt compressionOpt) {
    super(pos);
    this.columnList = columnList;
    this.compressionOpt = compressionOpt;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(columnList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("ROW");
    SqlWriter.Frame frame = writer.startList(
        SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlNode column : columnList) {
      writer.sep(",");
      column.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
    switch (compressionOpt) {
    case AUTO_COMPRESS:
      writer.keyword("AUTO COMPRESS");
      break;
    case NO_AUTO_COMPRESS:
      writer.keyword("NO AUTO COMPRESS");
      break;
    case NOT_SPECIFIED:
      break;
    }
  }

  public enum CompressionOpt {
    /**
     * Enable auto compress for the column specified.
     */
    AUTO_COMPRESS,

    /**
     * Disable auto compress for the column specified.
     */
    NO_AUTO_COMPRESS,

    /**
     * Auto compress preference not specified.
     */
    NOT_SPECIFIED,
  }
}
