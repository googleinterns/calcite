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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code ALTER PROCEDURE} statement.
 */
public class SqlAlterProcedure extends SqlAlter {
  public final SqlIdentifier procedureName;
  public final boolean languageSql;
  public final List<AlterProcedureWithOption> options;
  public final boolean local;
  public final boolean isTimeZoneNegative;
  public final String timeZoneString;

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("ALTER PROCEDURE", SqlKind.ALTER_PROCEDURE);

  /**
   * Creates an instance of {@code SqlAlterProcedure}.
   *
   * @param pos The parser position
   * @param procedureName Name of the procedure
   * @param languageSql If LANGUAGE SQL is specified
   * @param options List of WITH options
   * @param local If LOCAL was specified for AT TIME ZONE
   * @param isTimeZoneNegative True if sign before timeZoneString is "-"
   * @param timeZoneString String after AT TIME ZONE, may be null
   */
  public SqlAlterProcedure(SqlParserPos pos, String scope,
      SqlIdentifier procedureName, boolean languageSql,
      List<AlterProcedureWithOption> options, boolean local,
      boolean isTimeZoneNegative, String timeZoneString) {
    super(pos, scope);
    this.procedureName = Objects.requireNonNull(procedureName);
    this.languageSql = languageSql;
    this.options = Objects.requireNonNull(options);
    this.local = local;
    this.isTimeZoneNegative = isTimeZoneNegative;
    this.timeZoneString = timeZoneString;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("ALTER PROCEDURE");
    unparseAlterOperation(writer, leftPrec, rightPrec);
  }

  @Override protected void unparseAlterOperation(SqlWriter writer, int leftPrec,
      int rightPrec) {
    procedureName.unparse(writer, 0, 0);
    if (languageSql) {
      writer.keyword("LANGUAGE SQL");
    }
    writer.keyword("COMPILE");
    if (!options.isEmpty()) {
      writer.keyword("WITH");
      SqlWriter.Frame frame = writer.startList("", "");
      for (AlterProcedureWithOption option : options) {
        writer.sep(",");
        switch (option) {
        case SPL:
          writer.keyword("SPL");
          break;
        case NO_SPL:
          writer.keyword("NO SPL");
          break;
        case WARNING:
          writer.keyword("WARNING");
          break;
        case NO_WARNING:
          writer.keyword("NO WARNING");
          break;
        }
      }
      writer.endList(frame);
    }
    if (local) {
      writer.keyword("AT TIME ZONE LOCAL");
    } else if (timeZoneString != null) {
      writer.keyword("AT TIME ZONE");
      if (isTimeZoneNegative) {
        writer.print("-");
      }
      writer.literal(timeZoneString);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(procedureName);
  }

  public enum AlterProcedureWithOption {
    /**
     * Source text of SQL procedure is stored in a dictionary.
     */
    SPL,

    /**
     * Source text of SQL procedure is not stored in a dictionary.
     */
    NO_SPL,

    /**
     * Compilation warnings are returned.
     */
    WARNING,

    /**
     * Compilation warnings are not returned.
     */
    NO_WARNING,
  }
}
