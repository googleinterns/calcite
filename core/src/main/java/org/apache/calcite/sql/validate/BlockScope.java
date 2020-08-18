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
import org.apache.calcite.sql.SqlDeclareCondition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLabeledBlock;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlScriptingNode;

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
  public final Map<String, SqlDeclareCondition> conditionDeclarations;

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
    if (names.size() != 1) {
      super.resolve(names, nameMatcher, deep, resolved);
      return;
    }
    String name = names.get(0);
    if (conditionDeclarations.containsKey(name)) {
      SqlValidatorNamespace ns = validator.getNamespace(conditionDeclarations.get(name));
      Step path = Path.EMPTY.plus(ns.getRowType(), 0, names.get(0),
          StructKind.FULLY_QUALIFIED);
      resolved.found(ns, false, this, path, null);
      return;
    }
    if (block.label == null) {
      super.resolve(names, nameMatcher, deep, resolved);
      return;
    }
    String label = block.label.getSimple();
    if (nameMatcher.matches(names.get(0), label)) {
      SqlValidatorNamespace ns = validator.getNamespace(block);
      Step path = Path.EMPTY.plus(ns.getRowType(), 0, names.get(0),
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
   * @return The namespace for that SqlNode
   */
  private SqlValidatorNamespace findReferenceNamespace(SqlIdentifier name) {
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
    SqlValidatorNamespace ns = findReferenceNamespace(label);
    if (ns instanceof LabeledBlockNamespace) {
      return (SqlLabeledBlock) ns.getNode();
    }
    return null;
  }

  /**
   * Searches this scope and parent scopes for a {@link SqlDeclareCondition} that
   * matches the provided label.
   *
   * @param label The label of the block to find
   * @return The labeled block, returns null if label is not found
   */
  public SqlDeclareCondition findConditionDeclaration(SqlIdentifier label) {
    SqlValidatorNamespace ns = findReferenceNamespace(label);
    if (ns instanceof ConditionNamespace) {
      return (SqlDeclareCondition) ns.getNode();
    }
    return null;
  }

  /**
   *
   *
   * @param conditionDeclaration
   */
  public void addConditionDeclaration(SqlDeclareCondition conditionDeclaration) {
    String name = conditionDeclaration.conditionName.getSimple();
    conditionDeclarations.put(name, conditionDeclaration);
  }
}
