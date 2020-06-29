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
 * A <code>SqlTableAttribute</code> is a base class that can be used
 * to create custom options for the SQL CREATE TABLE function.
 *
 * <p>To customize table option unparsing, override the method
 * {@link #unparse(SqlWriter, int, int)}.
 */
public abstract class SqlTableAttribute {
  private final SqlParserPos pos;

  /**
   * Creates a {@code SqlTableAttribute}.
   *
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttribute(SqlParserPos pos) {
    this.pos = pos;
  }

  /** Writes a SQL representation of this table option to a writer. */
  public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);

  public SqlParserPos getParserPos() {
    return pos;
  }
}
