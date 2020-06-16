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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlIndex</code> is a class that can be used to create an index used by the
 * SQL CREATE TABLE function.
 */
public abstract class SqlIndex {
  protected final SqlParserPos pos;
  protected final SqlNodeList columns;
  protected final SqlIdentifier name;
  protected final boolean isUnique;

  protected SqlIndex(SqlParserPos pos, SqlNodeList columns, SqlIdentifier name, boolean isUnique) {
    this.pos = pos;
    this.columns = columns;
    this.name = name;
    this.isUnique = isUnique;
  }

  public abstract void unparse(SqlWriter writer, int leftPrec, int rightPrec);

  public SqlParserPos getParserPos() {
    return pos;
  }
}
