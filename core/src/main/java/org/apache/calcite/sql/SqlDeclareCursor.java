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
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parse tree for {@code SqlDeclareCursor} call.
 */
public class SqlDeclareCursor extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("DECLARE CURSOR", SqlKind.DECLARE_CURSOR);

  public final SqlIdentifier cursorName;
  public final CursorScrollType scrollType;
  public final CursorReturnType withReturnType;
  public final CursorReturnToType returnToType;
  public final boolean only;
  public final CursorUpdateType updateType;
  public final SqlNode cursorSpecification;
  public final SqlIdentifier statementName;
  public final SqlIdentifier preparedStatementName;
  public final SqlNode prepareFrom;

  /**
   * Creates a {@code SqlDeclareCursor}.
   *
   * @param pos The parser position, must not be null
   * @param cursorName The name of the cursor, must not be null
   * @param scrollType Whether the cursor can scroll backwards, must not be null
   * @param withReturnType Specifies if procedure returns result set, must not
   *                       be null
   * @param returnToType Specifies who the result set is returned to, must not
   *                     be null
   * @param only True if ONLY keyword is specified
   * @param updateType Specifies if the cursor can be updated, must not be null
   * @param cursorSpecification A SELECT statement
   * @param statementName The name of the statement if cursorSpecification is
   *                      not provided
   * @param prepareStatementName The statement name in the PREPARE clause
   * @param prepareFrom The SQL statement to be executed
   */
  public SqlDeclareCursor(SqlParserPos pos, SqlIdentifier cursorName,
      CursorScrollType scrollType, CursorReturnType withReturnType,
      CursorReturnToType returnToType, boolean only,
      CursorUpdateType updateType, SqlNode cursorSpecification,
      SqlIdentifier statementName, SqlIdentifier prepareStatementName,
      SqlNode prepareFrom) {
    super(pos);
    this.cursorName = Objects.requireNonNull(cursorName);
    this.scrollType = Objects.requireNonNull(scrollType);
    this.withReturnType = Objects.requireNonNull(withReturnType);
    this.returnToType = Objects.requireNonNull(returnToType);
    this.only = only;
    this.updateType = Objects.requireNonNull(updateType);
    this.cursorSpecification = cursorSpecification;
    this.statementName = statementName;
    this.preparedStatementName = prepareStatementName;
    this.prepareFrom = prepareFrom;
    if (statementName != null && preparedStatementName != null
        && !statementName.equals(prepareStatementName)) {
      throw SqlUtil.newContextException(
          prepareStatementName.getParserPosition(),
              RESOURCE.declareCursorStatementNameMismatch());
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(cursorName, cursorSpecification,
        statementName, prepareFrom);
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("DECLARE");
    cursorName.unparse(writer, 0, 0);
    switch (scrollType) {
    case SCROLL:
      writer.keyword("SCROLL");
      break;
    case NO_SCROLL:
      writer.keyword("NO SCROLL");
      break;
    default:
      break;
    }
    writer.keyword("CURSOR");
    switch (withReturnType) {
    case WITH_RETURN:
      writer.keyword("WITH RETURN");
      if (only) {
        writer.keyword("ONLY");
      }
      if (returnToType != CursorReturnToType.UNSPECIFIED) {
        writer.keyword("TO " + returnToType);
      }
      break;
    case WITHOUT_RETURN:
      writer.keyword("WITHOUT RETURN");
      break;
    default:
      break;
    }
    writer.keyword("FOR");
    if (cursorSpecification != null) {
      cursorSpecification.unparse(writer, 0, 0);
      switch (updateType) {
      case UPDATE:
        writer.keyword("FOR UPDATE");
        break;
      case READ_ONLY:
        writer.keyword("FOR READ ONLY");
        break;
      default:
        break;
      }
    } else {
      statementName.unparse(writer, 0, 0);
    }
    if (preparedStatementName != null) {
      writer.keyword("PREPARE");
      preparedStatementName.unparse(writer, 0, 0);
      writer.keyword("FROM");
      prepareFrom.unparse(writer, 0, 0);
    }
  }

  public enum CursorScrollType {
    /**
     * Allows the cursor to scroll forward or back.
     */
    SCROLL,

    /**
     * Cursor can only scroll forwards.
     */
    NO_SCROLL,

    /**
     * Scroll type is not specified.
     */
    UNSPECIFIED,
  }

  public enum CursorReturnType {
    /**
     * A result set is returned by the procedure.
     */
    WITH_RETURN,

    /**
     * No result sets are returned by the procedure.
     */
    WITHOUT_RETURN,

    /**
     * Return type is not specified.
     */
    UNSPECIFIED,
  }

  public enum CursorReturnToType {
    /**
     * Result set is returned to the caller.
     */
    CALLER,

    /**
     * Result set is returned to the client.
     */
    CLIENT,

    /**
     * Return type is not specified.
     */
    UNSPECIFIED,
  }

  public enum CursorUpdateType {
    /**
     * The cursor cannot be updated.
     */
    READ_ONLY,

    /**
     * The cursor can be updated.
     */
    UPDATE,

    /**
     * The update type is not specified.
     */
    UNSPECIFIED,
  }
}
