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
import org.apache.calcite.sql.validate.BlockScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code SqlSignal} call.
 */
public class SqlSignal extends SqlScriptingNode {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("SIGNAL", SqlKind.SIGNAL);

  public final SignalType signalType;
  public final SqlIdentifier conditionOrSqlState;
  public final SqlSetStmt setStmt;
  public SqlDeclareCondition conditionDeclaration;

  /**
   * Creates a {@code SqlSignal}.
   *
   * @param pos Parser position, must not be null
   * @param signalType Specifies if call is SIGNAL or RESIGNAL, must not be null
   * @param conditionOrSqlState A condition name or SQLSTATE value
   * @param setStmt The optional SET clause
   */
  public SqlSignal(SqlParserPos pos, SignalType signalType,
      SqlIdentifier conditionOrSqlState, SqlSetStmt setStmt) {
    super(pos);
    this.signalType = Objects.requireNonNull(signalType);
    this.conditionOrSqlState = conditionOrSqlState;
    this.setStmt = setStmt;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(conditionOrSqlState, setStmt);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(signalType.toString());
    if (conditionOrSqlState != null) {
      conditionOrSqlState.unparse(writer, 0, 0);
    }
    if (setStmt != null) {
      setStmt.unparse(writer, 0, 0);
    }
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    BlockScope bs = (BlockScope) scope;
    conditionDeclaration = bs.findConditionDeclaration(conditionOrSqlState);
  }

  public enum SignalType {
    /**
     * Raises an exception or condition.
     */
    SIGNAL,

    /**
     * Resignals or invokes a condition from a handler declaration.
     */
    RESIGNAL,
  }
}
