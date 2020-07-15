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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Base class for RENAME statements parse tree nodes.
 */
public abstract class SqlRename extends SqlDdl {

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

  private final SqlIdentifier targetTable;
  private final SqlIdentifier sourceTable;
  private final RenameOption renameOption;

  /** Creates a SqlRename. */
  protected SqlRename(SqlOperator operator, SqlParserPos pos,
      SqlIdentifier targetTable, SqlIdentifier sourceTable,
      RenameOption renameOption) {
    super(operator, pos);
    this.targetTable = targetTable;
    this.sourceTable = sourceTable;
    this.renameOption = renameOption;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetTable, sourceTable);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    targetTable.unparse(writer, leftPrec, rightPrec);
    writer.keyword(renameOption.toString());
    sourceTable.unparse(writer, leftPrec, rightPrec);
  }
}
