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
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Util;

import java.util.Locale;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code SqlHexCharStringLiteral} expression.
 */
public class SqlHexCharStringLiteral extends SqlLiteral {

  final HexCharLiteralFormat format;
  final CharacterSet charSet;

  public SqlHexCharStringLiteral(final String hex, final SqlParserPos pos,
      final String charSetString, final String formatString) {
    this(hex, pos, charSetString, formatString, SqlTypeName.CHAR);
  }

  /**
   * Creates a {@code SqlHexCharStringLiteral}.
   * @param hex           String that contains the hex characters
   * @param pos           Parser position, must not be null
   * @param charSetString Character set string, can be null
   * @param formatString  Format of the hex char literal
   * @param typeName The type, must be either CHAR or BYTE
   */
  public SqlHexCharStringLiteral(final String hex, final SqlParserPos pos,
      final String charSetString, final String formatString,
      final SqlTypeName typeName) {
    super(new NlsString(hex, null, null), typeName, pos);
    if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.BYTE) {
      throw Util.unexpected(typeName);
    }
    if (typeName == SqlTypeName.BYTE && hex.length() % 2 == 1) {
      throw new IllegalStateException("Must have an even number of hex digits "
          + "in a byte literal.");
    }
    if (charSetString == null) {
      this.charSet = null;
    } else {
      String normalizedCharSetString = SqlParserUtil
          .trim(charSetString, " ")
          .toUpperCase(Locale.ROOT);
      switch (normalizedCharSetString) {
      case "LATIN":
        charSet = CharacterSet.LATIN;
        break;
      case "UNICODE":
        charSet = CharacterSet.UNICODE;
        break;
      case "GRAPHIC":
        charSet = CharacterSet.GRAPHIC;
        break;
      case "KANJISJIS":
        charSet = CharacterSet.KANJISJIS;
        break;
      case "KANJI1":
        charSet = CharacterSet.KANJI1;
        break;
      default:
        throw SqlUtil.newContextException(pos,
            RESOURCE.unknownCharacterSet(charSetString));
      }
    }
    switch (formatString) {
    case "XC":
      format = HexCharLiteralFormat.XC;
      break;
    case "XCV":
      format = HexCharLiteralFormat.XCV;
      break;
    case "XB":
      format = HexCharLiteralFormat.XB;
      break;
    case "XBF":
      format = HexCharLiteralFormat.XBF;
      break;
    default:
      format = HexCharLiteralFormat.XCF;
      break;
    }
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    if (charSet != null) {
      writer.print("_");
      writer.keyword(charSet.toString());
    }
    writer.getDialect().unparseByteLiteral(writer, value.toString(),
        format.toString(), leftPrec, rightPrec);
  }

  public enum HexCharLiteralFormat {
    /**
     * Default format option, same as XCV.
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

    /**
     * BYTE format.
     */
    XB,

    /**
     * Fixed BYTE format.
     */
    XBF,
  }
}
