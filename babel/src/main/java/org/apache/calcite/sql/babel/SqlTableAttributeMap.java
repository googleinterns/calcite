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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlTableAttributeMap</code> is a table option
 * for the MAP attribute.
 */
public class SqlTableAttributeMap extends SqlTableAttribute {

  private final SqlIdentifier mapName;

  /**
   * Creates a {@code SqlTableAttributeMap}.
   *
   * @param mapName  Name of an existing contiguous map
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeMap(SqlIdentifier mapName, SqlParserPos pos) {
    super(pos);
    this.mapName = mapName;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    writer.keyword("MAP");
    writer.sep("=");
    mapName.unparse(writer, 0, 0);
  }
}
