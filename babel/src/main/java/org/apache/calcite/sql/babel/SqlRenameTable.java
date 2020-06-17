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
package org.apache.calcite.sql.babel;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for {@code RENAME TABLE} statement.
 */
public class SqlRenameTable extends SqlCall implements SqlExecutableStatement {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("RENAME TABLE", SqlKind.RENAME_TABLE);

  private final SqlIdentifier targetTable;
  private final SqlIdentifier sourceTable;
  private final RenameOption renameOption;

  /** Creates a SqlRename. */
  public SqlRenameTable(SqlParserPos pos, SqlIdentifier targetTable,
      SqlIdentifier sourceTable, RenameOption renameOption) {
    super(pos);
    this.targetTable = targetTable;
    this.sourceTable = sourceTable;
    this.renameOption = renameOption;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, sourceTable);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("RENAME TABLE");
    targetTable.unparse(writer, leftPrec, rightPrec);
    writer.keyword(renameOption.toString());
    sourceTable.unparse(writer, leftPrec, rightPrec);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}

  public enum RenameOption {
    /**
     * Query used the TO keyword.
     */
    TO,

    /**
     * Query used the AS keyword.
     */
    AS
  }
}
