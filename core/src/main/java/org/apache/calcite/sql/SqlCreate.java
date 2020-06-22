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
 * Base class for an CREATE statements parse tree nodes. The portion of the
 * statement covered by this class is "CREATE | REPLACE | CREATE OR REPLACE".
 * Subclasses handle whatever comes afterwards.
 * Support for REPLACE is included in this class for cases where REPLACE shares
 * the same syntax as CREATE.
 */
public abstract class SqlCreate extends SqlDdl {

  /** Whether "CREATE", "REPLACE", or "CREATE OR REPLACE" was specified. */
  final SqlCreateSpecifier createSpecifier;

  /** Whether "IF NOT EXISTS" was specified. */
  public final boolean ifNotExists;

  /**
   * Enum to indicate whether the query is a "CREATE", "CREATE OR REPLACE",
   * or "REPLACE" query.
   */
  public enum SqlCreateSpecifier {
    CREATE("CREATE"),
    REPLACE("REPLACE"),
    CREATE_OR_REPLACE("CREATE OR REPLACE");

    private final String name;

    SqlCreateSpecifier(String s) {
      name = s;
    }

    public boolean equalsName(String otherName) {
      return name.equals(otherName);
    }

    @Override public String toString() {
      return this.name;
    }
  }

  /** Creates a SqlCreate. */
  protected SqlCreate(SqlOperator operator, SqlParserPos pos,
      SqlCreateSpecifier createSpecifier, boolean ifNotExists) {
    super(operator, pos);
    this.createSpecifier = createSpecifier;
    this.ifNotExists = ifNotExists;
  }

  @Deprecated // to be removed before 2.0
  @SuppressWarnings("deprecation")
  protected SqlCreate(SqlParserPos pos, SqlCreateSpecifier createSpecifier) {
    this(SqlDdl.DDL_OPERATOR, pos, createSpecifier, false);
  }

  public SqlCreateSpecifier getCreateSpecifier() {
    return createSpecifier;
  }
}
