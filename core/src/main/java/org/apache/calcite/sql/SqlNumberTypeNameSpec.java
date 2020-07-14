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

  public final boolean isPrecisionStar;
  public final SqlLiteral precision;
  public final SqlLiteral scale;

  /**
   * Create a {@code SqlNumberTypeNameSpec} instance.
   *
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

  private boolean isValidPrecision(boolean isPrecisionStar, SqlLiteral precision) {
    if (isPrecisionStar || precision == null) {
      return true;
    }
    int numericPrecision = precision.getValueAs(Integer.class);
    return numericPrecision >= 1 && numericPrecision <= 38;
  }

  private boolean isValidScale(boolean isPrecisionStar, SqlLiteral precision,
      SqlLiteral scale) {
    if (scale == null) {
      return true;
    }
    int numericScale = scale.getValueAs(Integer.class);
    if (isPrecisionStar) {
      return numericScale >= 0 && numericScale <= 38;
    } else {
      int numericPrecision = precision.getValueAs(Integer.class);
      return numericScale >= 0 && numericScale <= numericPrecision;
    }
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
    if (!(spec instanceof SqlNumberTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }
    SqlNumberTypeNameSpec that = (SqlNumberTypeNameSpec) spec;
    if (this.isPrecisionStar != that.isPrecisionStar
        || this.precision.equalsDeep(that.precision, litmus)
        || this.scale.equalsDeep(that.scale, litmus)) {
      return litmus.fail("{} != {}", this, spec);
    }
    return litmus.succeed();
  }
}
