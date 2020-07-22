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
 * Parse tree for {@code SqlRangeNStartEnd} expression.
 */
public class SqlRangeNStartEnd extends SqlCall{
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("RANGE_N_AND ", SqlKind.OTHER);

  final SqlNode startLiteral;
  final SqlNode endLiteral;
  final SqlNode eachSizeLiteral;
  final boolean startAsterisk;
  final boolean endAsterisk;

  /**
   * Creates a {@code SqlRangeNStartEnd}.
   * @param pos             Parser position, must not be null
   * @param startLiteral    start value of the range
   * @param endLiteral      end value of the range
   * @param eachSizeLiteral step value within the range
   * @param startAsterisk   Is the start value an asterisk
   * @param endAsterisk     Is the end value an asterisk
   */
  public SqlRangeNStartEnd(final SqlParserPos pos, final SqlNode startLiteral
      , final SqlNode endLiteral, final SqlNode eachSizeLiteral
      , final boolean startAsterisk, final boolean endAsterisk) {
    super(pos);
    this.startLiteral = startLiteral;
    this.endLiteral = endLiteral;
    this.eachSizeLiteral = eachSizeLiteral;
    this.startAsterisk = startAsterisk;
    this.endAsterisk = endAsterisk;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(startLiteral, endLiteral, eachSizeLiteral);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec
      , final int rightPrec) {
    if (startAsterisk) {
      writer.keyword("*");
    } else {
      startLiteral.unparse(writer, leftPrec, rightPrec);
    }
    if (endLiteral != null) {
      writer.keyword("AND");
      endLiteral.unparse(writer, leftPrec, rightPrec);
    } else if (endAsterisk) {
      writer.keyword("AND *");
    }
    if (eachSizeLiteral != null) {
      writer.keyword("EACH");
      eachSizeLiteral.unparse(writer, leftPrec, rightPrec);
    }
  }
}
