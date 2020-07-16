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
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Parse tree for {@code RENAME TABLE} statement.
 */
public class SqlRenameTable extends SqlRename implements SqlExecutableStatement {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("RENAME TABLE", SqlKind.RENAME_TABLE);

  /** Creates a {@code SqlRenameTable}. */
  public SqlRenameTable(SqlParserPos pos, SqlIdentifier targetTable,
      SqlIdentifier sourceTable, RenameOption renameOption) {
    super(OPERATOR, pos, targetTable, sourceTable, renameOption);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}
}
