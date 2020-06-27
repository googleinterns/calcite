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
 * A <code>SqlTableAttributeChecksum</code> is a CREATE TABLE option
 * for the CHECKSUM attribute.
 */
public class SqlTableAttributeChecksum extends SqlTableAttribute {

  private final ChecksumEnabled checksumEnabled;

  /**
   * Creates a {@code SqlTableAttributeChecksum}.
   *
   * @param checksumEnabled  Status of checksums enabled for this table type
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeChecksum(ChecksumEnabled checksumEnabled, SqlParserPos pos) {
    super(pos);
    this.checksumEnabled = checksumEnabled;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("CHECKSUM");
    writer.sep("=");
    switch (checksumEnabled) {
    case DEFAULT:
      writer.keyword("DEFAULT");
      break;
    case ON:
      writer.keyword("ON");
      break;
    case OFF:
      writer.keyword("OFF");
      break;
    }
  }

  public enum ChecksumEnabled {
    /**
     * The current checksum level setting specified for this table type.
     */
    DEFAULT,

    /**
     * Checksums are enabled for this table type.
     */
    ON,

    /**
     * Checksums are disabled for this table type.
     */
    OFF,
  }
}
