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

import java.util.Objects;

/**
 * Parse tree for {@code SqlLabeledBlock} call.
 */
public abstract class SqlLabeledBlock extends SqlScriptingNode {

  public final SqlIdentifier label;
  public final SqlStatementList statements;

  /**
   * Creates an instance of {@code SqlLabeledBlock}.
   *
   * @param pos SQL parser position
   * @param label The label of the block
   * @param statements A list of statements inside the block, must not be null
   */
  protected SqlLabeledBlock(SqlParserPos pos, SqlIdentifier label,
      SqlStatementList statements) {
    super(pos);
    this.label = label;
    this.statements = Objects.requireNonNull(statements);
  }

  @Override public void validate(final SqlValidator validator,
      final SqlValidatorScope scope) {
    BlockScope bs = validator.getBlockScope(this);
    validateSqlNodeList(validator, bs, statements);
  }
}
