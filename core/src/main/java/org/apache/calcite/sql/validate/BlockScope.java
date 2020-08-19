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

import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlConditionDeclaration;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLabeledBlock;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Scope providing the objects that are in a block such as a BEGIN...END clause
 * or an iteration statement.
 */
public class BlockScope extends ListScope {

  public final SqlLabeledBlock block;
  public final Map<String, SqlConditionDeclaration> conditionDeclarations;

  /**
   * Creates an instance of {@code BlockScope}.
   *
   * @param parent  Parent scope, must not be null
   * @param block  May be a {@link org.apache.calcite.sql.SqlBeginEndCall} or a
   *               {@link org.apache.calcite.sql.SqlIterationStmt}, must not be
   *               null
   */
  public BlockScope(SqlValidatorScope parent, SqlLabeledBlock block) {
    super(parent);
    this.block = Objects.requireNonNull(block);
    conditionDeclarations = new HashMap<>();
  }

  @Override public SqlNode getNode() {
    return block;
  }

  @Override public void resolve(List<String> names, SqlNameMatcher nameMatcher,
      boolean deep, Resolved resolved) {
    // Labels and condition names are only valid as a single name. The other
    // cases, such as compound identifiers, will be handled by parent scopes.
    if (names.size() != 1) {
      super.resolve(names, nameMatcher, deep, resolved);
      return;
    }
    String name = names.get(0);
    SqlValidatorNamespace ns = null;
    if (block.label != null && nameMatcher.matches(name, block.label.getSimple())) {
      ns = validator.getNamespace(block);
    } else if (conditionDeclarations.containsKey(name)) {
      ns = validator.getNamespace(conditionDeclarations.get(name));
    }
    if (ns != null) {
      Step path = Path.EMPTY.plus(ns.getRowType(), 0, name,
          StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, this, path, null);
      return;
    }
    super.resolve(names, nameMatcher, deep, resolved);
  }

  /**
   * Searches this scope and parent scopes for a {@link SqlNode} that
   * matches the provided name.
   *
   * @param name The name of the SqlNode's namespace to find
   * @return The namespace for that SqlNode, returns null if label is not found
   */
  private SqlValidatorNamespace findNamespace(SqlIdentifier name) {
    SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
    SqlValidatorScope.ResolvedImpl resolved =
        new SqlValidatorScope.ResolvedImpl();
    List<String> names = new ArrayList<>();
    names.add(name.getSimple());
    resolve(names, nameMatcher, true, resolved);
    if (resolved.count() == 0) {
      return null;
    }
    return resolved.only().namespace;
  }

  /**
   * Searches this scope and parent scopes for a {@link SqlLabeledBlock} that
   * matches the provided label.
   *
   * @param label The label of the block to find
   * @return The labeled block, returns null if label is not found
   */
  public SqlLabeledBlock findLabeledBlock(SqlIdentifier label) {
    SqlValidatorNamespace ns = findNamespace(label);
    if (ns instanceof LabeledBlockNamespace) {
      return (SqlLabeledBlock) ns.getNode();
    }
    return null;
  }

  /**
   * Searches this scope and parent scopes for a {@link SqlConditionDeclaration}
   * that matches the provided name.
   *
   * @param name The name of the condition declaration to find
   * @return The condition declaration, returns null if label is not found
   */
  public SqlConditionDeclaration findConditionDeclaration(SqlIdentifier name) {
    SqlValidatorNamespace ns = findNamespace(name);
    if (ns instanceof ConditionDeclarationNamespace) {
      return (SqlConditionDeclaration) ns.getNode();
    }
    return null;
  }
}
