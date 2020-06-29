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
 * A <code>SqlTableAttributeIsolatedLoading</code> is a table option
 * for the WITH ISOLATED LOADING attribute.
 */
public class SqlTableAttributeIsolatedLoading extends SqlTableAttribute {

  private final boolean nonLoadIsolated;
  private final boolean concurrent;
  private final OperationLevel operationLevel;

  /**
   * Creates a {@code SqlTableAttributeIsolatedLoading}.
   *
   * @param nonLoadIsolated  Defines table as non-load isolated
   * @param concurrent  Allows concurrent read operations while table is being modified
   * @param operationLevel  Specifies types of operations allowed to be concurrent, may be null
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeIsolatedLoading(boolean nonLoadIsolated, boolean concurrent,
      OperationLevel operationLevel, SqlParserPos pos) {
    super(pos);
    this.nonLoadIsolated = nonLoadIsolated;
    this.concurrent = concurrent;
    this.operationLevel = operationLevel;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("WITH");
    if (nonLoadIsolated) {
      writer.keyword("NO");
    }
    if (concurrent) {
      writer.keyword("CONCURRENT");
    }
    writer.keyword("ISOLATED LOADING");
    if (operationLevel != null) {
      writer.keyword("FOR");
      switch (operationLevel) {
      case ALL:
        writer.keyword("ALL");
        break;
      case INSERT:
        writer.keyword("INSERT");
        break;
      case NONE:
        writer.keyword("NONE");
        break;
      }
    }
  }

  public enum OperationLevel {
    /**
     * All operations can be concurrent load isolated.
     */
    ALL,

    /**
     * Only insert operations can be concurrent load isolated.
     */
    INSERT,

    /**
     * No concurrent load operations are allowed.
     */
    NONE,
  }
}
