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

import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A {@code SqlAlterTableAttributeOnCommit} is a ALTER TABLE attribute
 * for the ON COMMIT attribute.
 */
public class SqlAlterTableAttributeOnCommit extends SqlTableAttribute {

  final OnCommitType onCommitType;

  /**
   * Creates a {@code SqlCreateOption}.
   * @param pos           Parser position, must not be null.
   * @param onCommitType  ON COMMIT option specified.
   */
  public SqlAlterTableAttributeOnCommit(SqlParserPos pos,
      OnCommitType onCommitType) {
    super(pos);
    this.onCommitType = onCommitType;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ON COMMIT");
    switch (onCommitType) {
    case DELETE:
      writer.keyword("DELETE");
      break;
    case PRESERVE:
      writer.keyword("PRESERVE");
      break;
    default:
      break;
    }
    writer.keyword("ROWS");
  }
}
