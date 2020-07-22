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
 * Parse tree for {@code SqlRangeN} expression.
 */
public class SqlRangeN extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("RANGE_N ", SqlKind.OTHER_FUNCTION);

  public final SqlNodeList rangeList;
  public final SqlNode testIdentifier;
  public final NoRangeUnknown extraPartitions;

  /**
   * Creates a {@code SqlRangeN}.
   * @param pos             Parser position, must not be null
   * @param testIdentifier  A value that the partition base on
   * @param rangeList       Range expressions
   * @param extraPartitions Represent extra partitions for no case and unknown
   */
  public SqlRangeN(final SqlParserPos pos, final SqlNode testIdentifier,
      final SqlNodeList rangeList, final NoRangeUnknown extraPartitions) {
    super(pos);
    this.testIdentifier = testIdentifier;
    this.rangeList = rangeList;
    this.extraPartitions = extraPartitions;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(testIdentifier, rangeList);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("RANGE_N");
    SqlWriter.Frame funcCallFrame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL,
        "(", ")");
    testIdentifier.unparse(writer, leftPrec, rightPrec);
    writer.keyword("BETWEEN");
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE,
        "", "");
    for (SqlNode range: rangeList.getList()) {
      writer.sep(",");
      range.unparse(writer, leftPrec, rightPrec);
    }
    if (extraPartitions != null) {
      writer.sep(",");
      switch (extraPartitions) {
      case NO_RANGE:
        writer.keyword("NO RANGE");
        break;
      case UNKNOWN:
        writer.keyword("UNKNOWN");
        break;
      case NO_RANGE_OR_UNKNOWN:
        writer.keyword("NO RANGE OR UNKNOWN");
        break;
      case NO_RANGE_COMMA_UNKNOWN:
        writer.keyword("NO RANGE");
        writer.sep(",");
        writer.keyword("UNKNOWN");
        break;
      }
    }
    writer.endList(frame);
    writer.endList(funcCallFrame);
  }

  public enum NoRangeUnknown {
    /**
     * An option to handle values that are not specified in rangeList.
     */
    NO_RANGE,

    /**
     * An option to handle values that are not specified in rangeList or null.
     */
    NO_RANGE_OR_UNKNOWN,

    /**
     * Options to handle values not in rangeList and null.
     */
    NO_RANGE_COMMA_UNKNOWN,

    /**
     * An option to handle null test_expression
     * , when it is not specified in rangeList.
     */
    UNKNOWN,
  }
}
