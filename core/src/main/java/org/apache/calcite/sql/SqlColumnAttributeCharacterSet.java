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
 * A <code>SqlColumnAttributeCharactetSet</code> is the column CHARACTER SET attribute.
 */
public class SqlColumnAttributeCharacterSet extends SqlColumnAttribute {

  private final CharacterSet characterSet;

  /**
   * Creates a {@code SqlColumnAttributeCharacterSet}.
   *
   * @param pos  Parser position, must not be null
   * @param characterSet  The specified character set
   */
  public SqlColumnAttributeCharacterSet(SqlParserPos pos, CharacterSet characterSet) {
    super(pos);
    this.charactetSet = characterSet
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CHARACTER SET");
    writer.keyword(characterSet.toString());
  }

  public enum CharacterSet {
    /**
     * Column has the LATIN character set
     */
    LATIN,

    /**
     * Column has the UNICODE character set
     */
    UNICODE,

    /**
     * Column has the GRAPHIC character set
     */
    GRAPHIC,

    /**
     * Column has the KANJISJIS character set
     */
    KANJISJIS,

    /**
     * Column has the KANJI character set
     */
    KANJI,
  }
}
