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
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.Objects;

/**
 * Parse tree for a {@code SqlConditionalStmt}.
 */
public abstract class SqlConditionalStmt extends SqlScriptingNode {

  public final SqlNodeList conditionalStmtListPairs;
  public final SqlNodeList elseStmtList;

  /**
   * Creates a {@code SqlConditionalStmt}.
   * @param pos                       Parser position, must not be null.
   * @param conditionalStmtListPairs  List of conditional expression pairs
   *                                  with StatementList, must not be null.
   * @param elseStmtList              List of statements in the else clause,
   *                                  must not be null.
   */
  protected SqlConditionalStmt(SqlParserPos pos,
      SqlNodeList conditionalStmtListPairs, SqlNodeList elseStmtList) {
    super(pos);
    this.conditionalStmtListPairs =
        Objects.requireNonNull(conditionalStmtListPairs);
    this.elseStmtList = Objects.requireNonNull(elseStmtList);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    validateSqlNodeList(validator, scope, conditionalStmtListPairs);
    validateSqlNodeList(validator, scope, elseStmtList);
  }
}
