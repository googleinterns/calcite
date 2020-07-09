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

/**
 * A {@code SqlAlterTableAttributeDataBlockSize} is a ALTER TABLE attribute
 * for the DATABLOCKSIZE attribute.
 */
public class SqlAlterTableAttributeDataBlockSize
    extends SqlTableAttributeDataBlockSize {

  final boolean immediate;

  /**
   * Creates a {@code SqlAlterTableAttributeDataBlockSize}.
   *
   * @param modifier      Data block size modifier, may be null.
   * @param unitSize      Unit size of a data block size value.
   * @param dataBlockSize Size of data block, numeric value.
   * @param pos           Parser position, must not be null.
   * @param immediate     Whether or not IMMEDIATE option was specified.
   */
  public SqlAlterTableAttributeDataBlockSize(DataBlockModifier modifier,
      DataBlockUnitSize unitSize, SqlLiteral dataBlockSize,
        SqlParserPos pos, boolean immediate) {
    super(modifier, unitSize, dataBlockSize, pos);
    this.immediate = immediate;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (immediate) {
      writer.keyword("IMMEDIATE");
    }
  }
}
