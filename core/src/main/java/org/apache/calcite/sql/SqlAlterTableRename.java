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
 * A {@code SqlAlterTableRename} represents a RENAME statement within an
 * ALTER TABLE query.
 */
public class SqlAlterTableRename extends SqlAlterTableOption {

  public final SqlIdentifier origName;
  public final SqlIdentifier newName;

  /**
   * Creates a {@code SqlAlterTableRename}.
   *
   * @param origName Original name of object to be renamed.
   * @param newName  New name of object to be renamed.
   */
  public SqlAlterTableRename(SqlIdentifier origName, SqlIdentifier newName) {
    this.origName = Objects.requireNonNull(origName);
    this.newName = Objects.requireNonNull(newName);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("RENAME");
    origName.unparse(writer, leftPrec, rightPrec);
    writer.keyword("TO");
    newName.unparse(writer, leftPrec, rightPrec);
  }
}
