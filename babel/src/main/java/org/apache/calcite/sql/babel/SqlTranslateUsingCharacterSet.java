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
package org.apache.calcite.sql.babel;

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlExecutableStatement;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A {@code SqlTranslateUsingCharacterSet} is an AST node that describes
 * the Translate Using CharacterSet to CharacterSet with Error.
 */
public class SqlTranslateUsingCharacterSet extends SqlCall implements SqlExecutableStatement {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("TRANSLATE ", SqlKind.OTHER_FUNCTION);

  private final List<SqlNode> args;
  private final boolean isWithError;

  /**
   * Creates a {@code SqlTranslateUsingCharacterSet}.
   *
   * @param pos  Parser position, must not be null
   * @param args  List of SqlNode, the first one is an char sequence to be
   *              translated, the second one is the AST node of SqlCharacterSetToCharacterSet
   * @param isWithError If the with error token is specified
   */
  public SqlTranslateUsingCharacterSet(final SqlParserPos pos, final List<SqlNode> args,
      final boolean isWithError) {
    super(pos);
    this.args = args;
    this.isWithError = isWithError;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(args.get(0), args.get(1));
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("TRANSLATE");
    writer.print("(");
    args.get(0).unparse(writer, 0, 0);
    writer.keyword("USING");
    args.get(1).unparse(writer, 0, 0);
    writer.setNeedWhitespace(true);
    if (isWithError) {
      writer.keyword("WITH ERROR");
    }
    writer.print(")");
  }

  // Intentionally left empty.
  @Override public void execute(final CalcitePrepare.Context context) {
  }
}
