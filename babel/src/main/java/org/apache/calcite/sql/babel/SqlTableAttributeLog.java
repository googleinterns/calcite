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
 * A <code>SqlTableAttributeLog</code> is a CREATE TABLE option
 * for the LOG attribute.
 */
public class SqlTableAttributeLog extends SqlTableAttribute {

  private final boolean loggingEnabled;

  /**
   * Creates a {@code SqlTableAttributeLog}.
   *
   * @param loggingEnabled  Transient journal logging is enabled
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeLog(boolean loggingEnabled, SqlParserPos pos) {
    super(pos);
    this.loggingEnabled = loggingEnabled;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (!loggingEnabled) {
      writer.keyword("NO");
    }
    writer.keyword("LOG");
  }
}
