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

public class SqlCaseN extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("CASE_N ", SqlKind.OTHER_FUNCTION);

  final SqlNodeList nodes;
  final NoCaseUnknown extraPartitions;

  public SqlCaseN (final SqlParserPos pos, final SqlNodeList nodes
      , final NoCaseUnknown extraPartitions) {
    super(pos);
    this.nodes = nodes;
    this.extraPartitions = extraPartitions;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(nodes);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("CASE_N");

    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL
        , "(", ")");
    for( SqlNode e : nodes.getList()){
      writer.sep(",");
      e.unparse(writer, leftPrec, rightPrec);
    }
    if (extraPartitions != null) {
      writer.sep(",");
      switch (extraPartitions) {
      case NO_CASE:
        writer.keyword("NO CASE");
        break;
      case UNKNOWN:
        writer.keyword("UNKNOWN");
        break;
      case NO_CASE_OR_UNKNOWN:
        writer.keyword("NO CASE OR UNKNOWN");
        break;
      case NO_CASE_COMMA_UNKNOWN:
        writer.keyword("NO CASE");
        writer.sep(",");
        writer.keyword("UNKNOWN");
        break;
      }
    }
    writer.endList(frame);
  }

  public enum NoCaseUnknown {
    NO_CASE,
    NO_CASE_OR_UNKNOWN,
    NO_CASE_COMMA_UNKNOWN,
    UNKNOWN
  }
}
