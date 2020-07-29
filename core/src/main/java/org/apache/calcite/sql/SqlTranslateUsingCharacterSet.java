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

import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * A {@code SqlTranslateUsingCharacterSet} is an AST node that describes
 * the Translate Using CharacterSet to CharacterSet with Error.
 */
public class SqlTranslateUsingCharacterSet extends SqlCall implements SqlExecutableStatement {
  public final List<SqlNode> args;
  public final boolean isTranslateChk;
  public final boolean isWithError;

  /**
   * Creates a {@code SqlTranslateUsingCharacterSet}.
   *
   * @param pos  Parser position, must not be null
   * @param args  List of SqlNode, the first one is an char sequence to be
   *              translated, the second one is the AST node of SqlCharacterSetToCharacterSet
   * @param isTranslateChk  If the TRANSLATE_CHK function should be used instead of TRANSLATE
   * @param isWithError  If the WITH ERROR tokens are specified
   */
  public SqlTranslateUsingCharacterSet(SqlParserPos pos, List<SqlNode> args, boolean isTranslateChk,
      boolean isWithError) {
    super(pos);
    this.args = args;
    this.isTranslateChk = isTranslateChk;
    this.isWithError = isWithError;
  }

  @Override public SqlOperator getOperator() {
    return isTranslateChk ? SqlStdOperatorTable.TRANSLATE_CHK : SqlStdOperatorTable.TRANSLATE;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(args.get(0), args.get(1));
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    if (isTranslateChk) {
      writer.keyword("TRANSLATE_CHK");
    } else {
      writer.keyword("TRANSLATE");
    }
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
