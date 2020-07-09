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
 * A {@code SqlAlterTableAttributeChecksum} is a ALTER TABLE attribute
 * for the CHECKSUM attribute.
 */
public class SqlAlterTableAttributeChecksum extends SqlTableAttributeChecksum {

  final boolean immediate;

  /**
   * Creates a {@code SqlAlterTableAttributeChecksum}.
   *
   * @param checksumEnabled Status of checksums enabled for this table type.
   * @param pos             Parser position, must not be null.
   * @param immediate       Whether or not IMMEDIATE option was specified.
   */
  public SqlAlterTableAttributeChecksum(ChecksumEnabled checksumEnabled,
      SqlParserPos pos, boolean immediate) {
    super(checksumEnabled, pos);
    this.immediate = immediate;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (immediate) {
      writer.keyword("IMMEDIATE");
    }
  }
}
