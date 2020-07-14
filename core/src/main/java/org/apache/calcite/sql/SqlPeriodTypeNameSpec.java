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
 * A sql type name specification of the PERIOD type.
 */
public class SqlPeriodTypeNameSpec extends SqlTypeNameSpec{

  public final TimeScale timeScale;
  public final Integer precision;
  public final boolean isWithTimezone;

  public SqlPeriodTypeNameSpec(TimeScale timeScale,
      SqlNumericLiteral precision,
      boolean isWithTimezone, SqlParserPos pos) {
    super(new SqlIdentifier("Period",pos), pos);

    // date period cannot contain precision or with time zone token
    if (timeScale == TimeScale.DATE
        && (precision != null || isWithTimezone)) {
      throw SqlUtil.newContextException(pos,
          RESOURCE.illegalNonQueryExpression());
    }

    if (precision != null) {
      if (!precision.isInteger()) {
        throw SqlUtil.newContextException(pos,
            RESOURCE.illegalNonQueryExpression());
      }
      int precisionValue = Integer.parseInt(precision.toValue());
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
    writer.keyword(timeScale.toString());
    if (precision != null) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.print(precision);
      writer.endList(frame);
    }
    if (isWithTimezone) {
      writer.keyword("WITH TIME ZONE");
    }
    writer.endList(periodFrame);
  }

  @Override public boolean equalsDeep(final SqlTypeNameSpec spec, final Litmus litmus) {
    if (!(spec instanceof SqlPeriodTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }

    SqlPeriodTypeNameSpec that = (SqlPeriodTypeNameSpec) spec;

    // two period types are the same as long as their time scales are the same
    if (this.timeScale != that.timeScale) {
      return litmus.fail("{} != {}", this, spec);
    }

    return litmus.succeed();
  }

  public enum TimeScale {
    /**
     * Time scale is DATE.
     */
    DATE,

    /**
     * Time scale is TIME.
     */
    TIME,

    /**
     * Time scale is TIMESTAMP.
     */
    TIMESTAMP
  }
}
