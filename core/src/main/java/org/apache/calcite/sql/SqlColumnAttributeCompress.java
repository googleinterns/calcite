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
 * A <code>SqlColumnAttributeCompress</code> is the column COMPRESS attribute.
 */
public class SqlColumnAttributeCompress extends SqlColumnAttribute {

  public final SqlNode value;

  /**
   * Creates a {@code SqlColumnAttributeCompress}.
   *
   * @param pos  Parser position, must not be null
   * @param value  Value(s) to be compressed in a particular column. These can
   *               be an individual string or numeric literal or a list
   *               of string literals, numeric literals, nulls, and DATEs.
   */
  public SqlColumnAttributeCompress(SqlParserPos pos, SqlNode value) {
    super(pos);
    this.value = value;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("COMPRESS");
    if (value == null) {
      return;
    }
    if (value instanceof SqlNodeList) {
      SqlNodeList values = (SqlNodeList) value;
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode v : values) {
        writer.sep(",", false);
        v.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    } else {
      value.unparse(writer, leftPrec, rightPrec);
    }
  }
}
