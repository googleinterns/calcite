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
 * Base class for {@code RENAME} statement parse tree nodes.
 */
public abstract class SqlRename extends SqlDdl {

  public final SqlIdentifier targetStructure;
  public final SqlIdentifier sourceStructure;

  /**
   * Creates a {@code SqlRename}.
   * @param operator   The specific RENAME operator
   * @param pos  Parser position, must not be null
   * @param targetStructure  The structure being renamed
   * @param sourceStructure  What the structure is being renamed to
   * @param renameOption  Whether the query was specified using TO or AS
   */
  protected SqlRename(SqlOperator operator, SqlParserPos pos,
      SqlIdentifier targetStructure, SqlIdentifier sourceStructure) {
    super(operator, pos);
    this.targetStructure = targetStructure;
    this.sourceStructure = sourceStructure;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(targetStructure, sourceStructure);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(getOperator().getName());
    targetStructure.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AS");
    sourceStructure.unparse(writer, leftPrec, rightPrec);
  }
}
