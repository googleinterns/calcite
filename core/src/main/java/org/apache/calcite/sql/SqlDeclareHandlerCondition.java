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

import java.util.Objects;

/**
 * Parse tree for {@code SqlDeclareHandlerCondition}. This class is used to
 * add the DeclareHandlerConditionType enum values to a list containing other
 * SqlIdentifier values.
 */
public class SqlDeclareHandlerCondition extends SqlIdentifier {

  public final DeclareHandlerConditionType conditionType;

  /**
   * Creates an instance of {@code SqlDeclareHandlerCondition}.
   *
   * @param pos Parser position, must not be null
   * @param conditionType A value from DeclareHandlerConditionType, must not be
   *                      null
   */
  public SqlDeclareHandlerCondition(SqlParserPos pos,
      DeclareHandlerConditionType conditionType) {
    super(conditionType.toString(), pos);
    this.conditionType = Objects.requireNonNull(conditionType);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    switch (conditionType) {
    case SQLEXCEPTION:
    case SQLWARNING:
      writer.keyword(conditionType.toString());
      break;
    case NOT_FOUND:
      writer.keyword("NOT FOUND");
      break;
    }
  }

  public enum DeclareHandlerConditionType {
    /**
     * Represents the SQLSTATE codes for all exception conditions.
     */
    SQLEXCEPTION,

    /**
     * Represents the SQLSTATE codes for all completion conditions.
     */
    SQLWARNING,

    /**
     * Represents the SQLSTATE codes for "no data found" conditions.
     */
    NOT_FOUND,
  }
}
