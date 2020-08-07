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
import org.apache.calcite.util.ImmutableNullableList;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * A <code>SqlDelete</code> is a node of a parse tree which represents a DELETE
 * statement.
 */
public class SqlDelete extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DELETE", SqlKind.DELETE);

  public final SqlIdentifier deleteTableName;
  public final SqlNodeList tables;
  public final SqlNodeList aliases;
  public final SqlNode condition;
  public SqlSelect sourceSelect;

  //~ Constructors -----------------------------------------------------------

  public SqlDelete(
      SqlParserPos pos,
      SqlIdentifier deleteTableName,
      SqlNode table,
      SqlIdentifier alias,
      SqlNode condition,
      SqlSelect sourceSelect) {
    this(pos, deleteTableName, new SqlNodeList(ImmutableList.of(table), pos),
        new SqlNodeList(ImmutableNullableList.of(alias), pos), condition,
        sourceSelect);
  }

  public SqlDelete(
      SqlParserPos pos,
      SqlIdentifier deleteTableName,
      SqlNodeList tables,
      SqlNodeList aliases,
      SqlNode condition,
      SqlSelect sourceSelect) {
    super(pos);
    this.deleteTableName = deleteTableName;
    this.tables = Objects.requireNonNull(tables);
    this.aliases = Objects.requireNonNull(aliases);
    this.condition = condition;
    this.sourceSelect = sourceSelect;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqlKind getKind() {
    return SqlKind.DELETE;
  }

  public SqlOperator getOperator() {
    return OPERATOR;
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(deleteTableName, tables, aliases, condition,
        sourceSelect);
  }

  public void setSourceSelect(SqlSelect sourceSelect) {
    this.sourceSelect = sourceSelect;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DELETE");
    if (deleteTableName != null) {
      deleteTableName.unparse(writer, leftPrec, rightPrec);
    }
    writer.keyword("FROM");
    final SqlWriter.Frame frame = writer.startList("", "");
    for (int i = 0; i < tables.size(); ++i) {
      writer.sep(",", /*printFirst=*/false);
      tables.get(i).unparse(writer, leftPrec, rightPrec);
      if (aliases != null && aliases.get(i) != null) {
        writer.keyword("AS");
        aliases.get(i).unparse(writer, leftPrec, rightPrec);
      }
    }
    writer.endList(frame);
    if (condition != null) {
      writer.newlineAndIndent();
      writer.keyword("WHERE");
      condition.unparse(writer, leftPrec, rightPrec);
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateDelete(this);
  }
}
