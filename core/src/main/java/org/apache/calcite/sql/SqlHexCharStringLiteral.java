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


public class SqlHexCharStringLiteral extends SqlLiteral {

  final HexCharLiteralFormat format;
  final String charSet;

  /**
   * Creates a <code>SqlLiteral</code>.
   * @param value
   * @param typeName
   * @param pos
   */
  public SqlHexCharStringLiteral(final NlsString value, final SqlTypeName typeName,
      final SqlParserPos pos, final String charSet, final HexCharLiteralFormat format) {
    super(value, typeName, pos);
    this.charSet = charSet;
    this.format = format;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    if (charSet != null) {
      writer.print("_");
      writer.keyword(charSet);
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
     * XCV.
     */
    XCV,

    /**
     * XCF.
     */
    XCF,
  }
}
