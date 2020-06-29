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
 * A {@code SqlTableAttributeFreeSpace} is a ALTER TABLE attribute
 * for the FREESPACE attribute.
 */
public class SqlAlterTableAttributeFreeSpace
    extends SqlTableAttributeFreeSpace {

  final boolean isDefault;

  /**
   * Creates a {@code SqlAlterTableAttributeFreeSpace}.
   *
   * @param freeSpaceValue The percentage of free space to reserve
   *                          during loading operations.
   * @param percent        Optional keyword PERCENT.
   * @param pos            Parser position, must not be null.
   * @param isDefault      Whether DEFAULT FREESPACE option was specified.
   */
  public SqlAlterTableAttributeFreeSpace(int freeSpaceValue, boolean percent,
      SqlParserPos pos, boolean isDefault) {
    super(freeSpaceValue, percent, pos);
    this.isDefault = isDefault;
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (isDefault) {
      writer.keyword("DEFAULT FREESPACE");
    } else {
      super.unparse(writer, leftPrec, rightPrec);
    }
  }
}
