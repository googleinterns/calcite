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
 * A <code>SqlColumnAttributeNullable</code> is the column NULL attribute.
 */
public class SqlColumnAttributeNullable extends SqlColumnAttribute {

  private final boolean isNullable;

  /**
   * Creates a {@code SqlColumnAttributeNullable}.
   *
   * @param isNullable  Whether or not the column is nullable or not
   * @param pos  Parser position, must not be null
   */
  public SqlColumnAttributeNullable(SqlParserPos pos, boolean isNullable) {
    super(pos);
    this.isNullable = isNullable;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    // If isNullable is true, we don't explicitly unparse NULL
    if (!isNullable) {
      writer.keyword("NOT NULL");
    }
  }
}
