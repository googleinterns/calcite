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
 * A sql type name specification of the JSON type.
 */
public class SqlJsonTypeNameSpec extends SqlTypeNameSpec {

  public final Integer maxLength;
  public final Integer inlineLength;
  public final CharacterSet characterSet;
  public final StorageFormat storageFormat;

  public SqlJsonTypeNameSpec(SqlParserPos pos,
      Integer maxLength,
      Integer inlineLength,
      CharacterSet characterSet,
      StorageFormat storageFormat) {
    super(new SqlIdentifier("JSON", pos), pos);
    this.maxLength = maxLength;
    this.inlineLength = inlineLength;
    this.characterSet = characterSet;
    this.storageFormat = storageFormat;

    // These two fields are mutally exclusive.
    if (characterSet != null && storageFormat != null) {
      throw SqlUtil.newContextException(pos,
          RESOURCE.illegalQueryExpression());
    }

    // Only LATIN and UNICODE are valid for json.
    if (characterSet != null && characterSet != CharacterSet.LATIN
        && characterSet != CharacterSet.UNICODE) {
      throw SqlUtil.newContextException(pos,
          RESOURCE.illegalQueryExpression());
    }
  }

  @Override public RelDataType deriveType(SqlValidator validator) {
    return validator.getValidatedNodeType(getTypeName());
  }

  @Override public boolean equalsDeep(SqlTypeNameSpec spec, Litmus litmus) {
    if (!(spec instanceof SqlJsonTypeNameSpec)) {
      return litmus.fail("{} != {}", this, spec);
    }
    SqlJsonTypeNameSpec that = (SqlJsonTypeNameSpec) spec;

    if ((this.maxLength == null || that.maxLength == null)
        && !this.maxLength.equals(that.maxLength)) {
      return litmus.fail("{} != {}", this, spec);
    }

    if ((this.inlineLength == null || that.inlineLength == null)
        && !this.inlineLength.equals(that.inlineLength)) {
      return litmus.fail("{} != {}", this, spec);
    }

    if (!this.maxLength.equals(that.maxLength)
        || !this.inlineLength.equals(that.inlineLength)
        || this.characterSet != that.characterSet
        || this.storageFormat != that.storageFormat) {
      return litmus.fail("{} != {}", this, spec);
    }
    return litmus.succeed();
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("JSON");
    if (maxLength != null) {
      final SqlWriter.Frame frame =
          writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      writer.print(maxLength);
      writer.endList(frame);
    }
    if (inlineLength != null) {
      writer.keyword("INLINE LENGTH " + inlineLength);
    }
    if (characterSet != null) {
      writer.keyword("CHARACTER SET " + characterSet.toString());
    } else if (storageFormat != null) {
      writer.keyword("STORAGE FORMAT " + storageFormat.toString());
    }
  }

  public enum StorageFormat {
    /**
     * Storage format is Binary Json.
     */
    BSON,

    /**
     * Storage format is Universal Binary Json.
     */
    UBJSON
  }
}
