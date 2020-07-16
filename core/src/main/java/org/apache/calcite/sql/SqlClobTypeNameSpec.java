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
 * A SQL type name specification for the CLOB data type.
 */
public class SqlClobTypeNameSpec extends SqlTypeNameSpec {

  public final SqlLiteral maxLength;
  public final SqlLobUnitSize unitSize;
  public final CharacterSet characterSet;

  /**
   * Create a {@code SqlClobTypeNameSpec} instance.
   *
   * @param maxLength The number of bytes to allocate for CLOB column
   * @param unitSize The unit size of maxLength
   * @param characterSet The character set of the column
   * @param pos The parser position
   */
  public SqlClobTypeNameSpec(SqlLiteral maxLength, SqlLobUnitSize unitSize,
      CharacterSet characterSet, SqlParserPos pos) {
    super(new SqlIdentifier("CLOB", pos), pos);
    this.maxLength = maxLength;
    this.unitSize = unitSize;
    this.characterSet = characterSet;
    if (maxLength != null && characterSet != null
        && !isValidMaxLength(maxLength, unitSize, characterSet)) {
      throw SqlUtil.newContextException(maxLength.getParserPosition(),
        RESOURCE.numberLiteralOutOfRange(String.valueOf(maxLength)));
    }
  }

  private static boolean isValidMaxLength(SqlLiteral maxLength,
      SqlLobUnitSize unitSize, CharacterSet characterSet) {
    int numericMaxLength = maxLength.getValueAs(Integer.class);
    if (numericMaxLength == 0) {
      return false;
    }
    switch (characterSet) {
    case LATIN:
      if (unitSize == SqlLobUnitSize.UNSPECIFIED
          && numericMaxLength > 2097088000) {
        return false;
      } else if (unitSize == SqlLobUnitSize.K && numericMaxLength > 2047937) {
        return false;
      } else if (unitSize == SqlLobUnitSize.M && numericMaxLength > 1999) {
        return false;
      } else if (unitSize == SqlLobUnitSize.G && numericMaxLength > 1) {
        return false;
      }
      break;
    case UNICODE:
      if (unitSize == SqlLobUnitSize.UNSPECIFIED
          && numericMaxLength > 1048544000) {
        return false;
      } else if (unitSize == SqlLobUnitSize.K && numericMaxLength > 1023968) {
        return false;
      } else if (unitSize == SqlLobUnitSize.M && numericMaxLength > 999) {
        return false;
      } else if (unitSize == SqlLobUnitSize.G) {
        return false;
      }
      break;
    default:
      return false;
    }
    return true;
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CLOB");
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
    if (characterSet != null) {
      writer.keyword("CHARACTER SET " + characterSet);
    }
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    return false;
  }
}
