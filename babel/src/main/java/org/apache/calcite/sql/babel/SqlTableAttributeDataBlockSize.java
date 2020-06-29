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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlTableAttributeDataBlockSize</code> is a table option
 * for the DATABLOCKSIZE attribute.
 */
public class SqlTableAttributeDataBlockSize extends SqlTableAttribute {

  private final DataBlockModifier modifier;
  private final DataBlockUnitSize unitSize;
  private final SqlLiteral dataBlockSize;

  /**
   * Creates a {@code SqlTableAttributeDataBlockSize}.
   *
   * @param modifier  Data block size modifier, may be null
   * @param unitSize  Unit size of a data block size value
   * @param dataBlockSize  Size of data block, numeric value
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeDataBlockSize(DataBlockModifier modifier,
      DataBlockUnitSize unitSize, SqlLiteral dataBlockSize, SqlParserPos pos) {
    super(pos);
    this.modifier = modifier;
    this.unitSize = unitSize;
    this.dataBlockSize = dataBlockSize;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    if (modifier != null) {
      switch (modifier) {
      case DEFAULT:
        writer.keyword("DEFAULT");
        break;
      case MINIMUM:
        writer.keyword("MINIMUM");
        break;
      case MAXIMUM:
        writer.keyword("MAXIMUM");
        break;
      }
      writer.keyword("DATABLOCKSIZE");
    } else {
      writer.keyword("DATABLOCKSIZE");
      writer.sep("=");
      dataBlockSize.unparse(writer, 0, 0);
      switch (unitSize) {
      case BYTES:
        writer.keyword("BYTES");
        break;
      case KILOBYTES:
        writer.keyword("KILOBYTES");
        break;
      }
    }
  }

  public enum DataBlockModifier {
    /**
     * The default data block size.
     */
    DEFAULT,

    /**
     * The minimum data block size for this table.
     */
    MINIMUM,

    /**
     * The maximum data block size for this table.
     */
    MAXIMUM,
  }

  public enum DataBlockUnitSize {
    /**
     * The data block size value is in bytes. Defaults to bytes if no unit size is specified.
     */
    BYTES,

    /**
     * The data block size value is in kilobytes.
     */
    KILOBYTES,
  }
}
