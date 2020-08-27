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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;



/**
 * Parse tree for a {@code SqlByteLiteral}.
 */
public class SqlByteLiteral extends SqlLiteral {

  public final String format;

  /**
   * Creates a {@code SqlByteLiteral}.
   *
   * @param hex The hex string, can not be null
   * @param pos The parser position, can not be null
   * @param format The format of the byte
   */
  public SqlByteLiteral(final String hex, final SqlParserPos pos,
      final String format) {
    super(new NlsString(hex, null, null), SqlTypeName.BYTE, pos);
    if (hex.length() % 2 == 1) {
      throw new IllegalStateException("Must have an even number of hex digits "
          + "in a byte literal.");
    }
    this.format = format;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.getDialect().unparseByteLiteral(writer, this, leftPrec, rightPrec);
  }
}
