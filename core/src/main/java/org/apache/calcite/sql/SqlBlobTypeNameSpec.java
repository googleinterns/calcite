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
 * A SQL type name specification for the BLOB data type.
 */
public class SqlBlobTypeNameSpec extends SqlTypeNameSpec {

  public final SqlLiteral maxLength;
  public final SqlLobUnitSize unitSize;

  /**
   * Create a {@code SqlBlobTypeNameSpec} instance.
   *
   * @param maxLength The number of bytes to allocate for BLOB column
   * @param unitSize The unit size of maxLength
   * @param pos The parser position
   */
  public SqlBlobTypeNameSpec(SqlLiteral maxLength, SqlLobUnitSize unitSize,
      SqlParserPos pos) {
    super(new SqlIdentifier("BLOB", pos), pos);
    this.maxLength = maxLength;
    this.unitSize = unitSize;
    if (maxLength != null && !isValidMaxLength(maxLength, unitSize)) {
      throw SqlUtil.newContextException(maxLength.getParserPosition(),
        RESOURCE.numberLiteralOutOfRange(String.valueOf(maxLength)));
    }
  }

  private static boolean isValidMaxLength(SqlLiteral maxLength, SqlLobUnitSize unitSize) {
    int numericMaxLength = maxLength.getValueAs(Integer.class);
    if (numericMaxLength == 0) {
      return false;
    }
    switch (unitSize) {
    case UNSPECIFIED:
      if (numericMaxLength > 2097088000) {
        return false;
      }
      break;
    case K:
      if (numericMaxLength > 2047937) {
        return false;
      }
      break;
    case M:
      if (numericMaxLength > 1999) {
        return false;
      }
      break;
    case G:
      if (numericMaxLength > 1) {
        return false;
      }
      break;
    }
    return true;
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("BLOB");
    if (maxLength != null) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      maxLength.unparse(writer, 0, 0);
      if (unitSize != SqlLobUnitSize.UNSPECIFIED) {
        writer.setNeedWhitespace(false);
        writer.print(unitSize.toString());
      }
      writer.endList(frame);
    }
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    return false;
  }
}
