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
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A {@code SqlDateTimeAtTimeZone} is an AST node that describes
 * the date time expression of At Time Zone.
 */
public class SqlDateTimeAtTimeZone extends SqlCall {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("AT TIME ZONE", SqlKind.OTHER);

  public final SqlNode dateTimePrimary;
  public final SqlNode displacementValue;

  /**
   * Creates a {@code SqlDateTimeAtTimeZone}.
   *
   * @param pos  Parser position, must not be null
   * @param dateTimePrimary  SqlNode, contains the time to be transformed
   * @param displacementValue  SqlNode, contains the displacement to the time
   */
  public SqlDateTimeAtTimeZone(
      SqlParserPos pos,
      SqlNode dateTimePrimary,
      SqlNode displacementValue) {
    super(pos);
    this.dateTimePrimary = dateTimePrimary;
    this.displacementValue = displacementValue;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(dateTimePrimary, displacementValue);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    dateTimePrimary.unparse(writer, leftPrec, rightPrec);
    writer.keyword("AT TIME ZONE");
    displacementValue.unparse(writer, leftPrec, rightPrec);
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public void validate(SqlValidator validator,
      SqlValidatorScope scope) {
    dateTimePrimary.validate(validator, scope);
  }
}
