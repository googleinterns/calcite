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

/**
 * Parse tree for {@code SqlScriptingNode} call.
 */
public abstract class SqlScriptingNode extends SqlCall {

  public SqlScriptingNode(SqlParserPos pos) {
    super(pos);
  }

  @Override public void validate(SqlValidator validator,
      final SqlValidatorScope scope) {
    // Left empty so that scripting statements such as cursor calls are not
    // validated.
  }

  /**
   * Validates a list of statements only if validation is supported for its
   * SqlNode type.
   *
   * @param validator The validator
   * @param scope The current scope for this node
   * @param statements The list of statements to validate
   */
  public void validateStatementList(SqlValidator validator,
      SqlValidatorScope scope, SqlNodeList statements) {
    for (SqlNode statement : statements) {
      if (statement instanceof SqlScriptingNode
          || statement instanceof SqlSelect
          || statement instanceof SqlInsert
          || statement instanceof SqlUpdate
          || statement instanceof SqlMerge
          || statement instanceof SqlDelete) {
        statement.validate(validator, scope);
      }
    }
  }
}
