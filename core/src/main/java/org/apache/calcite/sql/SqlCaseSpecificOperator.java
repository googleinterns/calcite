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

import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Operator for an inline CASESPECIFIC statement.
 */
public class SqlCaseSpecificOperator extends SqlPostfixOperator {
  final boolean includeNot;

  /**
   * Create an {@code SqlCaseSpecificOperator}.
   *
   * @param kind        The SqlKind of the Operator.
   * @param prec        Precedence.
   * @param returnTypeInference   Inferred return type.
   * @param operandTypeInference  Inferred operand type.
   * @param operandTypeChecker    Operand type checker.
   * @param includeNot  Specifies if NOT was parsed before CASE SPECIFIC.
   */
  public SqlCaseSpecificOperator(SqlKind kind,
      int prec,
      SqlReturnTypeInference returnTypeInference,
      SqlOperandTypeInference operandTypeInference,
      SqlOperandTypeChecker operandTypeChecker,
      boolean includeNot) {
    super(includeNot ? "NOT CASESPECIFIC" : "CASESPECIFIC",
        kind, prec, returnTypeInference, operandTypeInference,
        operandTypeChecker);
    this.includeNot = includeNot;
  }

  @Override public void unparse(final SqlWriter writer, final SqlCall call,
      final int leftPrec, final int rightPrec) {
    call.getOperandList().get(0).unparse(writer, leftPrec, rightPrec);
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE, "(", ")");
    if (includeNot) {
      writer.keyword("NOT");
    }
    writer.keyword("CASESPECIFIC");
    writer.endList(frame);
  }
}
