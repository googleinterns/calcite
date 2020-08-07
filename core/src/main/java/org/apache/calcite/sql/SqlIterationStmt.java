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
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for a {@code SqlIterationStmt}.
 */
public abstract class SqlIterationStmt extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("ITERATION", SqlKind.ITERATION_STATEMENT);

  public final SqlNode condition;
  public final SqlStatementList statements;
  public final SqlIdentifier label;

  /**
   * Creates a {@code SqlIterationStmt}.
   *
   * @param pos         Parser position, must not be null.
   * @param condition   Conditional expression, can be null for loop statement
   *                    only.
   * @param statements  List of statements to iterate, must not be null.
   * @param beginLabel  Optional begin label, must match end label if not null.
   * @param endLabel    Optional end label, must match begin label if not null.
   */
  protected SqlIterationStmt(final SqlParserPos pos, final SqlNode condition,
      final SqlStatementList statements, final SqlIdentifier beginLabel,
      final SqlIdentifier endLabel) {
    super(pos);
    if (endLabel != null && (beginLabel == null
            || !beginLabel.equalsDeep(endLabel, Litmus.IGNORE))) {
      throw SqlUtil.newContextException(endLabel.getParserPosition(),
          RESOURCE.beginEndLabelMismatch());
    }
    if (endLabel == null && beginLabel != null) {
      throw SqlUtil.newContextException(beginLabel.getParserPosition(),
          RESOURCE.beginEndLabelMismatch());
    }
    this.condition = condition;
    this.statements = Objects.requireNonNull(statements);
    this.label = beginLabel;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(condition, statements, label);
  }
}
