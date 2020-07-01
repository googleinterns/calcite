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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;

import java.util.Objects;

/**
 * A {@code SqlAlterTableAddColumns} represents an ADD column statement within
 * an ALTER TABLE query.
 */
public class SqlAlterTableAddColumns extends SqlAlterTableOption {

  public final SqlNodeList columns;

  /*
   * Creates a {@code SqlAlterTableAddColumns}.
   * @param columns  The list of columns to add. Must be non-null and non-empty
   */
  public SqlAlterTableAddColumns(SqlNodeList columns) {
    this.columns = Objects.requireNonNull(columns);
  }

  @Override public void unparse(SqlWriter writer,
      int leftPrec, int rightPrec) {
    writer.keyword("ADD");
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlNode c : columns) {
      writer.sep(",");
      c.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }
}
