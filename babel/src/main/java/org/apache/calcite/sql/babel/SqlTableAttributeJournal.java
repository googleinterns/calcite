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
 * A <code>SqlTableAttributeJournal</code> is a CREATE TABLE option
 * for the JOURNAL attribute.
 */
public class SqlTableAttributeJournal extends SqlTableAttribute {

  private final JournalType journalType;
  private final JournalModifier journalModifier;

  /**
   * Creates a {@code SqlTableAttributeJournal}.
   *
   * @param journalType  Type of journal image to be maintained
   * @param journalModifier  Journal image modifier
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeJournal(JournalType journalType,
      JournalModifier journalModifier,
      SqlParserPos pos) {
    super(pos);
    this.journalType = journalType;
    this.journalModifier = journalModifier;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    switch (journalModifier) {
    case NO:
      writer.keyword("NO");
      break;
    case DUAL:
      writer.keyword("DUAL");
      break;
    case LOCAL:
      writer.keyword("LOCAL");
      break;
    case NOT_LOCAL:
      writer.keyword("NOT");
      writer.keyword("LOCAL");
      break;
    default:
    }
    switch (journalType) {
    case BEFORE:
      writer.keyword("BEFORE");
      break;
    case AFTER:
      writer.keyword("AFTER");
      break;
    default:
    }
    writer.keyword("JOURNAL");
  }

  public enum JournalType {
    /**
     * Maintains both types of journal images.
     */
    UNSPECIFIED,

    /**
     * Maintains a before change image.
     */
    BEFORE,

    /**
     * Maintains an after change image.
     */
    AFTER,
  }

  public enum JournalModifier {
    /**
     * No modifiers specified.
     */
    UNSPECIFIED,

    /**
     * A journal image is not maintained.
     */
    NO,

    /**
     * Two journal images are maintained.
     */
    DUAL,

    /**
     * Single after-image journal rows are written locally.
     */
    LOCAL,

    /**
     * Single after-image journal rows are not written locally.
     */
    NOT_LOCAL,
  }
}
