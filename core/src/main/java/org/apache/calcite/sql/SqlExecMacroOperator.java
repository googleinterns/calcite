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

/**
 * SqlExecMacroOperator represents the EXECUTE statement. It takes a single
 * operand which is the real SqlCall.
 */
public class SqlExecMacroOperator extends SqlPrefixOperator {

  public SqlExecMacroOperator() {
    super("EXECUTE", SqlKind.EXECUTE, 0, null, null, null);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    SqlBasicCall execCall = (SqlBasicCall) call.getOperandList().get(0);
    SqlUnresolvedFunction function = (SqlUnresolvedFunction) execCall.getOperator();
    // If a macro has no parameters it should be unparsed without parentheses.
    if (execCall.operandCount() == 0) {
      writer.keyword(getName());
      function.getNameAsId().unparse(writer, leftPrec, rightPrec);
    } else {
      super.unparse(writer, call, leftPrec, rightPrec);
    }
  }
}
