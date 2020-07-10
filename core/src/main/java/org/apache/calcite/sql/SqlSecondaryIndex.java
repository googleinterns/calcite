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
 * A {@code SqlSecondaryIndex} is a class that can be used to create a
 * non-primary index, which is used by the SQL CREATE TABLE function.
 */
public class SqlSecondaryIndex extends SqlIndex {
  public SqlSecondaryIndex(SqlParserPos pos, SqlNodeList columns,
      SqlIdentifier name, boolean isUnique) {
    super(pos, columns, name, isUnique);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (isUnique) {
      writer.keyword("UNIQUE");
    }
    writer.keyword("INDEX");
    if (name != null) {
      name.unparse(writer, leftPrec, rightPrec);
    }
    if (columns != null) {
      SqlWriter.Frame frame = writer.startList("(", ")");
      for (SqlNode c : columns) {
        writer.sep(",");
        c.unparse(writer, 0, 0);
      }
      writer.endList(frame);
    }
  }
}
