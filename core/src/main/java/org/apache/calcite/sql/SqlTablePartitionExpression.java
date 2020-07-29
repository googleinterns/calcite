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
 * Parse tree for Partition Expression.
 */
public class SqlTablePartitionExpression extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("PARTITION_EXPRESSION",
          SqlKind.OTHER);
  public final SqlNode partitionExpression;
  public final int extraNumberOfPartitions;

  /**
   * Creates a {@code SqlTablePartitionExpression}.
   * @param pos                     Parser position, must not be null.
   * @param partitionExpression     Partition expressions in a SqlNodeList.
   * @param extraNumberOfPartitions Big Integer represent the extra number
   *                                of Partitions, when it is 0,
   *                                it is not specified.
   */
  public SqlTablePartitionExpression(final SqlParserPos pos,
      final SqlNode partitionExpression, final int extraNumberOfPartitions) {
    super(pos);
    this.partitionExpression = partitionExpression;
    this.extraNumberOfPartitions = extraNumberOfPartitions;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(partitionExpression);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    partitionExpression.unparse(writer, leftPrec, rightPrec);
    if (extraNumberOfPartitions == 0) {
      return;
    }
    writer.keyword("ADD");
    writer.print(extraNumberOfPartitions);
  }
}
