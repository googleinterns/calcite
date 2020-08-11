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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Parse tree for {@code SqlFirstLastValue} statement.
 */
public class SqlFirstLastValue extends SqlCall implements SqlExecutableStatement {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("FIRST_LAST_VALUE", SqlKind.FIRST_LAST_VALUE);

  public final boolean first;
  public final SqlNode value;
  public final NullOption nullOption;

  /**
   * Creates a {@code SqlFirstLastOption}.
   *
   * @param pos  Parser position, must not be null
   * @param first  Whether or not this is the FIRST_VALUE or LAST_VALUE
   * @param value  The value which is being used
   * @param nullOption  If the nulls should be ignored or respected
   */
  public SqlFirstLastValue(SqlParserPos pos, boolean first, SqlNode value,
      NullOption nullOption) {
    super(pos);
    this.first = first;
    this.value = Objects.requireNonNull(value);
    this.nullOption = Objects.requireNonNull(nullOption);
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(value);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (first) {
      writer.keyword("FIRST_VALUE");
    } else {
      writer.keyword("LAST_VALUE");
    }
    SqlWriter.Frame frame = writer.startList("(", ")");
    value.unparse(writer, leftPrec, rightPrec);
    if (nullOption != NullOption.UNSPECIFIED) {
      writer.keyword(nullOption.toString());
      writer.keyword("NULLS");
    }
    writer.endList(frame);
  }

  // Intentionally left empty.
  @Override public void execute(CalcitePrepare.Context context) {}

  public enum NullOption {
    /**
     * Nulls should be ignored.
     */
    IGNORE,

    /**
     * Nulls should be respected.
     */
    RESPECT,

    /**
     * Unspecified.
     */
    UNSPECIFIED,
  }
}
