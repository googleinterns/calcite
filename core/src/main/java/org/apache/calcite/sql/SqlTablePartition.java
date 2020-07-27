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
 * Parse tree for Partition By clause in Create Table.
 */
public class SqlTablePartition extends SqlCall{
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PARTITION BY", SqlKind.OTHER);
  final public SqlNodeList partitionExpressions;

  /**
   * Creates a {@code SqlTablePartition}.
   * @param pos         Parser position, must not be null.
   * @param partitionExpressions  Partition expressions in a SqlNodeList.
   */
  public SqlTablePartition (final SqlParserPos pos,
      final SqlNodeList partitionExpressions) {
    super(pos);
    this.partitionExpressions = partitionExpressions;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(partitionExpressions);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("PARTITION BY");
    SqlWriter.Frame frame = writer.startList(
        SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlNode partitionExpression : partitionExpressions) {
      writer.sep(",");
      partitionExpression.unparse(writer, leftPrec, rightPrec);
    }
    writer.endList(frame);
  }
}
