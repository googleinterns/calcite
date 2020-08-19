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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code SqlDeclareHandler} call.
 */
public class SqlDeclareHandler extends SqlConditionDeclaration {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DECLARE_HANDLER", SqlKind.DECLARE_HANDLER);

  public final HandlerType handlerType;
  public final SqlNodeList parameters;
  public final SqlNode handlerStatement;
  public List<SqlConditionDeclaration> conditionDeclarations;

  /**
   * Creates a {@code SqlDeclareHandler}.
   *
   * @param pos Parser position, must not be null
   * @param handlerType The type of handler being declared, must not be null
   * @param conditionName Name of condition before CONDITION keyword
   * @param parameters List of SQLSTATE values, or
   *                   {@link SqlDeclareHandlerCondition} objects and
   *                   identifiers, must not be null
   * @param handlerStatement Handler action statement
   */
  public SqlDeclareHandler(SqlParserPos pos, HandlerType handlerType,
      SqlIdentifier conditionName, SqlNodeList parameters,
      SqlNode handlerStatement) {
    super(pos, conditionName);
    this.handlerType = Objects.requireNonNull(handlerType);
    this.parameters = Objects.requireNonNull(parameters);
    this.handlerStatement = handlerStatement;
    conditionDeclarations = new ArrayList<>();
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(conditionName, parameters,
        handlerStatement);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DECLARE");
    switch (handlerType) {
    case CONTINUE:
    case EXIT:
      writer.keyword(handlerType + " HANDLER");
      break;
    case CONDITION:
      conditionName.unparse(writer, 0, 0);
      writer.keyword("CONDITION");
      break;
    }
    if (SqlNodeList.isEmptyList(parameters)) {
      return;
    }
    writer.keyword("FOR");
    parameters.unparse(writer, 0, 0);
    if (handlerStatement != null) {
      handlerStatement.unparse(writer, 0, 0);
    }
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    BlockScope bs = (BlockScope) scope;
    for (SqlNode node : parameters) {
      if (!(node instanceof SqlIdentifier)) {
        continue;
      }
      SqlConditionDeclaration declaration
          = bs.findConditionDeclaration((SqlIdentifier) node);
      if (declaration != null) {
        conditionDeclarations.add(declaration);
      }
    }
  }

  public enum HandlerType {
    /**
     * Passes control to the next statement in the BEGIN...END clause.
     */
    CONTINUE,

    /**
     * Passes control to next statement outside BEGIN...END clause.
     */
    EXIT,

    /**
     * Specifies conditions for a handler to act on when the condition name is
     * used in SIGNAL or RESIGNAL statement.
     */
    CONDITION,
  }
}
