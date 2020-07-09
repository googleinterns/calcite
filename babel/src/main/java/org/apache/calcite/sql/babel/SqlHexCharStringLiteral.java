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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

/**
 * Parse tree for {@code SqlHexCharStringLiteral} expression.
 */
public class SqlHexCharStringLiteral extends SqlLiteral {

  final HexCharLiteralFormat format;
  final BabelCharacterSet charSet;

  /**
   * Creates a {@code SqlHexCharStringLiteral}.
   * @param value
   * @param typeName
   * @param pos
   */
  public SqlHexCharStringLiteral(final NlsString value, final SqlTypeName typeName,
      final SqlParserPos pos, final BabelCharacterSet charSet,
      final HexCharLiteralFormat format) {
    super(value, typeName, pos);
    this.charSet = charSet;
    this.format = format;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    if (charSet != null) {
      writer.print("_");
      switch (this.charSet) {
      case LATIN:
        writer.keyword("LATIN");
        break;
      case UNICODE:
        writer.keyword("UNICODE");
        break;
      case GRAPHIC:
        writer.keyword("GRAPHIC");
        break;
      case KANJISJIS:
        writer.keyword("KANJISJIS");
        break;
      }
    }
    writer.keyword(value.toString());
    switch (this.format) {
    case XC:
      writer.keyword("XC");
      break;
    case XCF:
      writer.keyword("XCF");
      break;
    case XCV:
      writer.keyword("XCV");
      break;
    default:
      break;
    }
  }

  public enum HexCharLiteralFormat {
    /**
     * Default to be the same as XCV.
     */
    XC,

    /**
     * VARCHAR format.
     */
    XCV,

    /**
     * CHAR format.
     */
    XCF,
  }

  public enum BabelCharacterSet {
    /**
     * Column has the LATIN character set.
     */
    LATIN,

    /**
     * Column has the UNICODE character set.
     */
    UNICODE,

    /**
     * Column has the GRAPHIC character set.
     */
    GRAPHIC,

    /**
     * Column has the KANJISJIS character set.
     */
    KANJISJIS,
  }
}
