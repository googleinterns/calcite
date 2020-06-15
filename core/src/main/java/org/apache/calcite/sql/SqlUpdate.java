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
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A <code>SqlUpdate</code> is a node of a parse tree which represents an UPDATE
 * statement.
 */
public class SqlUpdate extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("UPDATE", SqlKind.UPDATE);

  SqlNode targetTable;
  SqlNodeList targetColumnList;
  SqlNodeList sourceExpressionList;
  SqlNode condition;
  SqlSelect sourceSelect;
  SqlIdentifier alias;
  SqlNodeList sourceTables;
  SqlNodeList sourceAliases;

  //~ Constructors -----------------------------------------------------------

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias) {
    this(pos, targetTable, targetColumnList, sourceExpressionList, condition, sourceSelect, alias,
        /*sourceTables=*/null, /*sourceAliases=*/null);
  }

  public SqlUpdate(SqlParserPos pos,
      SqlNode targetTable,
      SqlNodeList targetColumnList,
      SqlNodeList sourceExpressionList,
      SqlNode condition,
      SqlSelect sourceSelect,
      SqlIdentifier alias,
      SqlNodeList sourceTables,
      SqlNodeList sourceAliases) {
    super(pos);
    this.targetTable = targetTable;
    this.targetColumnList = targetColumnList;
    this.sourceExpressionList = sourceExpressionList;
    this.condition = condition;
    this.sourceSelect = sourceSelect;
    assert sourceExpressionList.size() == targetColumnList.size();
    this.alias = alias;
    this.sourceTables = sourceTables;
    this.sourceAliases = sourceAliases;
    if (sourceTables != null) {
      assert this.sourceAliases != null
      assert this.sourceTables.size() == this.sourceAliases.size();
    }
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.UPDATE;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, targetColumnList,
        sourceExpressionList, condition, alias, sourceTables, sourceAliases);
  }

  @Override public void setOperand(int i, SqlNode operand) {
    switch (i) {
    case 0:
      assert operand instanceof SqlIdentifier;
      targetTable = operand;
      break;
    case 1:
      targetColumnList = (SqlNodeList) operand;
      break;
    case 2:
      sourceExpressionList = (SqlNodeList) operand;
      break;
    case 3:
      condition = operand;
      break;
    case 4:
      sourceExpressionList = (SqlNodeList) operand;
      break;
    case 5:
      alias = (SqlIdentifier) operand;
      break;
    case 6:
      sourceTables = (SqlNodeList) operand;
      break;
    case 7:
      sourceAliases = (SqlNodeList) operand;
      break;
    default:
      throw new AssertionError(i);
    }
  }

  /**
   * @return the identifier for the target table of the update
   */
  public SqlNode getTargetTable() {
    return targetTable;
  }

  /**
   * @return the alias for the target table of the update
   */
  public SqlIdentifier getAlias() {
    return alias;
  }

  public void setAlias(SqlIdentifier alias) {
    this.alias = alias;
  }

  /**
   * @return the list of target column names
   */
  public SqlNodeList getTargetColumnList() {
    return targetColumnList;
  }

  /**
   * @return the list of source expressions
   */
  public SqlNodeList getSourceExpressionList() {
    return sourceExpressionList;
  }

  /**
   * Gets the filter condition for rows to be updated.
   *
   * @return the condition expression for the data to be updated, or null for
   * all rows in the table
   */
  public SqlNode getCondition() {
    return condition;
  }

  /**
   * Gets the source SELECT expression for the data to be updated. Returns
   * null before the statement has been expanded by
   * {@link SqlValidatorImpl#performUnconditionalRewrites(SqlNode, boolean)}.
   *
   * @return the source SELECT for the data to be updated
   */
  public SqlSelect getSourceSelect() {
    return sourceSelect;
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  /**
   * @return the source table names
   */
  public SqlNodeList getSourceTables() {
    return sourceTables;
  }

  /**
   * @return the source table aliases
   */
  public SqlNodeList getSourceAliases() {
    return sourceAliases;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.getDialect().unparseSqlUpdateCall(writer, this, leftPrec, rightPrec);
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateUpdate(this);
  }
}
