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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.schema.impl.ViewTableMacro;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code CREATE VIEW} statement.
 */
public class SqlCreateView extends SqlCreate
    implements SqlExecutableStatement {
  public final SqlIdentifier name;
  public final SqlNodeList columnList;
  public final SqlNode query;
  public final SqlLiteral withCheckOption;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

  /** Creates a SqlCreateView (withCheckOption not specified). */
  SqlCreateView(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query) {
    this(pos, createSpecifier, name, columnList, query,
        /*withCheckOption=*/ false);
  }

  /** Creates a SqlCreateView (withCheckOption specified). */
  SqlCreateView(SqlParserPos pos, SqlCreateSpecifier createSpecifier,
      SqlIdentifier name, SqlNodeList columnList, SqlNode query,
      boolean withCheckOption) {
    super(OPERATOR, pos, createSpecifier, false);
    this.name = Objects.requireNonNull(name);
    this.columnList = columnList; // may be null
    this.query = Objects.requireNonNull(query);
    this.withCheckOption = SqlLiteral.createBoolean(withCheckOption, pos);
  }

  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(name, columnList, query, withCheckOption);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getCreateSpecifier().toString());
    writer.keyword("VIEW");
    name.unparse(writer, leftPrec, rightPrec);
    if (columnList != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columnList) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
    writer.keyword("AS");
    writer.newlineAndIndent();
    query.unparse(writer, 0, 0);
    if (withCheckOption.getValueAs(Boolean.class)) {
      writer.keyword("WITH CHECK OPTION");
    }
  }

  public void execute(CalcitePrepare.Context context) {
    final Pair<CalciteSchema, String> pair =
        SqlDdlNodes.schema(context, true, name);
    final SchemaPlus schemaPlus = pair.left.plus();
    for (Function function : schemaPlus.getFunctions(pair.right)) {
      if (function.getParameters().isEmpty()) {
        if (getCreateSpecifier() != SqlCreateSpecifier.CREATE_OR_REPLACE) {
          throw SqlUtil.newContextException(name.getParserPosition(),
              RESOURCE.viewExists(pair.right));
        }
        pair.left.removeFunction(pair.right);
      }
    }
    final SqlNode q = SqlDdlNodes.renameColumns(columnList, query);
    final String sql = q.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
    final ViewTableMacro viewTableMacro =
        ViewTable.viewMacro(schemaPlus, sql, pair.left.path(null),
            context.getObjectPath(), false);
    final TranslatableTable x = viewTableMacro.apply(ImmutableList.of());
    Util.discard(x);
    schemaPlus.add(pair.right, viewTableMacro);
  }

}
