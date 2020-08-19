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
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlConditionDeclaration;
import org.apache.calcite.sql.SqlNode;

import java.util.Objects;

/**
 * Namespace for a condition declaration inside a DECLARE CONDITION statement or
 * a condition handler declaration.
 */
public class ConditionDeclarationNamespace extends AbstractNamespace {

  private final SqlConditionDeclaration conditionDeclaration;

  /**
   * Creates a {@code ConditionNamespace}.
   *
   * @param validator Validator
   * @param conditionDeclaration The condition declaration, must not be null
   * @param enclosingNode Enclosing node
   */
  ConditionDeclarationNamespace(SqlValidatorImpl validator,
      SqlConditionDeclaration conditionDeclaration,
      SqlNode enclosingNode) {
    super(validator, enclosingNode);
    this.conditionDeclaration = Objects.requireNonNull(conditionDeclaration);
  }

  @Override protected RelDataType validateImpl(RelDataType targetRowType) {
    return getValidator().getUnknownType();
  }

  @Override public SqlNode getNode() {
    return conditionDeclaration;
  }
}
