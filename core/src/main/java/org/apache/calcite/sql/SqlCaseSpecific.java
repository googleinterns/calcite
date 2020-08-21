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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code SqlCaseSpecific} statement.
 */
public class SqlCaseSpecific extends SqlCall
    implements SqlExecutableStatement {

  private final boolean not;
  private final SqlNode value;

  /**
   * Create an {@code SqlCaseSpecific}.
   *
   * @param pos  Parser position, must not be null
   * @param not  Specifies if NOT was parsed before CASE SPECIFIC
   * @param value  The value preceding ([NOT] CASEPECIFIC)
   */
  public SqlCaseSpecific(SqlParserPos pos, boolean not, SqlNode value) {
    super(pos);
    this.not = not;
    this.value = Objects.requireNonNull(value);
  }

  @Override public SqlOperator getOperator() {
    return SqlStdOperatorTable.CASE_SPECIFIC;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(value);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    value.unparse(writer, leftPrec, rightPrec);
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
    if (not) {
      writer.keyword("NOT");
    }
    writer.keyword("CASESPECIFIC");
    writer.endList(frame);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
//
//  public <R> R accept(SqlVisitor<R> visitor) {
//    return visitor.visit(this);
//  }
}
