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
 * Parse tree for {@code SqlConditionDeclaration} call. If the child is a
 * {@link SqlDeclareHandler}, the {@code conditionName} may be null since the
 * name is not required in a handler statement.
 */
public abstract class SqlConditionDeclaration extends SqlScriptingNode {

  public final SqlIdentifier conditionName;

  /**
   * Creates an instance of {@code SqlConditionDeclaration}.
   *
   * @param pos SQL parser position
   * @param conditionName The label of the block, may be null
   */
  protected SqlConditionDeclaration(SqlParserPos pos,
      SqlIdentifier conditionName) {
    super(pos);
    this.conditionName = conditionName;
  }
}
