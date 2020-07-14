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

public class SqlPeriodTypeNameSpec extends SqlTypeNameSpec{

  public final TimeScale timeScale;
  public final Integer precision;
  public final Boolean isWithTimezone;

  public SqlPeriodTypeNameSpec(TimeScale timeScale,
      SqlNumericLiteral precision,
      Boolean isWithTimezone, SqlParserPos pos) {
    super(new SqlIdentifier("Period",pos), pos);

    if (timeScale == TimeScale.DATE
        && (precision != null || isWithTimezone != null)) {
      throw SqlUtil.newContextException(pos,
          RESOURCE.illegalNonQueryExpression());
    }

    if (precision != null) {
      if (!precision.isInteger()) {
        throw SqlUtil.newContextException(pos,
            RESOURCE.illegalNonQueryExpression());
      }
      Integer precisionValue = Integer.getInteger(precision.toValue());
      if (precisionValue < 0 || precisionValue > 6) {
        throw SqlUtil.newContextException(pos,
            RESOURCE.numberLiteralOutOfRange("Precision"));
      }
      this.precision = precisionValue;
    } else {
      this.precision = null;
    }

    this.timeScale = timeScale;
    this.isWithTimezone = isWithTimezone;
  }

  @Override public RelDataType deriveType(final SqlValidator validator) {
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("PERIOD");
    final SqlWriter.Frame periodFrame =
        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");

    if (precision != null) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.print(precision);
      writer.endList(frame);
    }

    if (isWithTimezone != null) {
      writer.keyword("WITH TIME ZONE");
    }

    writer.endList(periodFrame);
  }

  @Override public boolean equalsDeep(final SqlTypeNameSpec spec, final Litmus litmus) {
    if (!(spec instanceof SqlPeriodTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }

    SqlPeriodTypeNameSpec that = (SqlPeriodTypeNameSpec) spec;

    if (this.timeScale != that.timeScale) {
      return litmus.fail("{} != {}", this, spec);
    }

    return litmus.succeed();
  }

  public enum TimeScale {
    DATE,
    TIME,
    TIMESTAMP
  }
}