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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A SQL type name specification for the NUMBER data type.
 */
public class SqlNumberTypeNameSpec extends SqlTypeNameSpec {

  private final static int MIN_PRECISION = 1;
  private final static int MIN_SCALE = 0;
  private final static int MAX_INPUT = 38;
  public final boolean isPrecisionStar;
  public final SqlLiteral precision;
  public final SqlLiteral scale;

  /**
   * Create a {@code SqlNumberTypeNameSpec} instance.
   *
   * @param isPrecisionStar Whether the precision element is the symbol "*"
   * @param precision The precision, an unsigned integer in the range [1-38].
   *                  May be null
   * @param scale The scale, an unsigned integer in the range [0-38] if
   *              isPrecisionStar = true. If isPrecisionStar = false, the valid
   *              range is [0, precision]. May be null
   * @param pos The parser position
   */
  public SqlNumberTypeNameSpec(boolean isPrecisionStar, SqlLiteral precision,
      SqlLiteral scale, SqlParserPos pos) {
    super(new SqlIdentifier("NUMBER", pos), pos);
    this.isPrecisionStar = isPrecisionStar;
    this.precision = precision;
    this.scale = scale;
    if (!isValidPrecision(isPrecisionStar, precision)) {
      throw SqlUtil.newContextException(precision.getParserPosition(),
          RESOURCE.numberLiteralOutOfRange(String.valueOf(precision)));
    }
    if (!isValidScale(isPrecisionStar, precision, scale)) {
      throw SqlUtil.newContextException(scale.getParserPosition(),
          RESOURCE.numberLiteralOutOfRange(String.valueOf(scale)));
    }
  }

  private boolean isValidPrecision(boolean isPrecisionStar,
      SqlLiteral precision) {
    if (isPrecisionStar || precision == null) {
      return true;
    }
    int numericPrecision = precision.getValueAs(Integer.class);
    return numericPrecision >= MIN_PRECISION && numericPrecision <= MAX_INPUT;
  }

  private boolean isValidScale(boolean isPrecisionStar, SqlLiteral precision,
      SqlLiteral scale) {
    if (scale == null) {
      return true;
    }
    int numericScale = scale.getValueAs(Integer.class);
    int maxNumericScale = isPrecisionStar ? MAX_INPUT : precision.getValueAs(Integer.class);
    return numericScale >= MIN_SCALE && numericScale <= maxNumericScale;
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("NUMBER");
    if (!isPrecisionStar && precision == null) {
      return;
    }
    writer.setNeedWhitespace(false);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    if (isPrecisionStar) {
      writer.print("*");
    } else {
      precision.unparse(writer, 0, 0);
    }
    if (scale != null) {
      writer.sep(",", true);
      scale.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    return false;
  }
}
