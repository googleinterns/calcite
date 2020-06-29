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
 * A <code>SqlTableAttributeBlockCompression</code> is a table option
 * for the BLOCKCOMPRESSION attribute.
 */
public class SqlTableAttributeBlockCompression extends SqlTableAttribute {

  private final BlockCompressionOption blockCompressionOption;

  /**
   * Creates a {@code SqlTableAttributeBlockCompression}.
   *
   * @param blockCompressionOption  The block-level compression option for a table
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeBlockCompression(BlockCompressionOption blockCompressionOption,
      SqlParserPos pos) {
    super(pos);
    this.blockCompressionOption = blockCompressionOption;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("BLOCKCOMPRESSION");
    writer.sep("=");
    switch (blockCompressionOption) {
    case DEFAULT:
      writer.keyword("DEFAULT");
      break;
    case AUTOTEMP:
      writer.keyword("AUTOTEMP");
      break;
    case MANUAL:
      writer.keyword("MANUAL");
      break;
    case NEVER:
      writer.keyword("NEVER");
      break;
    }
  }

  public enum BlockCompressionOption {
    /**
     * Table uses the default block compression setting.
     */
    DEFAULT,

    /**
     * Table uses default block compression setting based on virtual storage temperature.
     */
    AUTOTEMP,

    /**
     * Table uses default block compression setting at time of creation.
     */
    MANUAL,

    /**
     * The table is not block-level compressed.
     */
    NEVER,
  }
}
