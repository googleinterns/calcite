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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;

import java.util.Objects;

/**
 * A {@code SqlAlterTableDrop} represents a DROP statement within an
 * ALTER TABLE query.
 */
public class SqlAlterTableDrop extends SqlAlterTableOption {

  public final SqlIdentifier dropObj;
  public final boolean identity;

  /**
   * Creates a {@code SqlAlterTableDrop}.
   *
   * @param dropObj   Identifier specifying object to drop.
   * @param identity  Whether or not IDENTITY keyword is specified.
   */
  public SqlAlterTableDrop(SqlIdentifier dropObj, boolean identity) {
    this.dropObj = Objects.requireNonNull(dropObj);
    this.identity = identity;
  }

  @Override public void unparse(SqlWriter writer,
      int leftPrec, int rightPrec) {
    writer.keyword("DROP");
    dropObj.unparse(writer, leftPrec, rightPrec);
    if (identity) {
      writer.keyword("IDENTITY");
    }
  }
}
