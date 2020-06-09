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
 * A <code>SqlCreateAttributeFreeSpace</code> is a CREATE TABLE option
 * for the FREESPACE attribute.
 */
public class SqlCreateAttributeFreeSpace extends SqlCreateAttribute {

  private int freeSpaceValue;
  private boolean percent;

  /**
   * Creates a {@code SqlCreateAttributeFreeSpace}.
   *
   * @param freeSpaceValue  The percentage of free space to reserve during loading operations.
   * @param percent  Optional keyword percent
   * @param pos  Parser position, must not be null
   */
  public SqlCreateAttributeFreeSpace(int freeSpaceValue, boolean percent, SqlParserPos pos) {
    super(pos);
    this.freeSpaceValue = freeSpaceValue;
    this.percent = percent;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("FREESPACE");
    writer.sep("=");
    writer.print(freeSpaceValue);
    writer.print(" ");
    if (percent) {
      writer.keyword("PERCENT");
    }
  }
}
