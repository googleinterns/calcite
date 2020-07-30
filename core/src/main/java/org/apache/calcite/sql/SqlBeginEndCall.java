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
import org.apache.calcite.util.Litmus;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

public class SqlBeginEndCall extends SqlCall {
  private static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("BEGIN_END ", SqlKind.BEGIN_END);

  public final SqlIdentifier label;
  public final SqlStatementList statements;

  public SqlBeginEndCall(SqlParserPos pos, SqlIdentifier beginLabel,
      SqlIdentifier endLabel, SqlStatementList statements) {
    super(pos);
    this.label = beginLabel;
    this.statements = statements;
    if (endLabel != null && !beginLabel.equalsDeep(endLabel, Litmus.IGNORE)) {
      throw SqlUtil.newContextException(endLabel.getParserPosition(),
          RESOURCE.beginEndLabelMismatch());
    }
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (label != null) {
      label.unparse(writer, 0, 0);
      writer.setNeedWhitespace(false);
      writer.print(": ");
    }
    writer.keyword("BEGIN");
    writer.newlineAndIndent();
    statements.unparse(writer, 0, 0);
    writer.keyword("END");
    if (label != null) {
      label.unparse(writer, 0, 0);
    }
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(label, statements);
  }
}
