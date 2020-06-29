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
 * A <code>SqlTableAttributeFallback</code> is a table option
 * for the FALLBACK keyword.
 */
public class SqlTableAttributeFallback extends SqlTableAttribute {

  private final boolean no;
  private final boolean protection;

  /**
   * Creates a {@code SqlTableAttributeFallback}.
   *
   * @param no  Optional NO keyword
   * @param protection Optional PROTECTION keyword
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeFallback(boolean no, boolean protection, SqlParserPos pos) {
    super(pos);
    this.no = no;
    this.protection = protection;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (no) {
      writer.keyword("NO");
    }
    writer.keyword("FALLBACK");
    if (protection) {
      writer.keyword("PROTECTION");
    }
  }
}
