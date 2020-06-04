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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlCreateAttributeJournalTable</code> is a CREATE TABLE option
 * for the WITH JOURNAL TABLE attribute
 */
public class SqlCreateAttributeJournalTable extends SqlCreateAttribute {

  private final SqlIdentifier sourceName; // Can be null
  private final SqlIdentifier tableName;

  /**
   * Creates a {@code SqlCreateAttributeJournalTable}.
   *
   * @param sourceName Optional name of database or user name, may be null
   * @param tableName  Name of the permanent journal table to be used, must not be null
   * @param pos  Parser position, must not be null
   */
  public SqlCreateAttributeJournalTable(SqlIdentifier sourceName,
      SqlIdentifier tableName,
      SqlParserPos pos) {
    super(pos);
    this.sourceName = sourceName;
    this.tableName = tableName;
  }

  public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("WITH");
    writer.keyword("JOURNAL");
    writer.keyword("TABLE");
    writer.sep("=");
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    if (sourceName != null) {
      sourceName.unparse(writer, 0, 0);
      writer.sep(".");
    }
    tableName.unparse(writer, 0, 0);
    writer.endList(frame);
  }

  public SqlIdentifier getSourceName() {
    return sourceName;
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }
}
