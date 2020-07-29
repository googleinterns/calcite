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

public class SqlHelpProcedure extends SqlHelp {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("HELP PROCEDURE", SqlKind.HELP_PROCEDURE);

  public final boolean attributes;

  /**
   * Creates a {@code SqlHelpProcedure}.
   * @param pos  Parser position, must not be null
   * @param name  Name of the procedure
   * @param attributes  True if ATTRIBUTES keyword is present
   */
  public SqlHelpProcedure(SqlParserPos pos, SqlIdentifier name,
      boolean attributes) {
    super(OPERATOR, pos, name);
    this.attributes = attributes;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    super.unparse(writer, leftPrec, rightPrec);
    if (attributes) {
      writer.keyword("ATTRIBUTES");
    }
  }
}
