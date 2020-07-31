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

import java.util.List;

public class SqlIfStmt extends SqlCall {
  public final SqlNodeList conditionalStmtListPairs;
  public final SqlNodeList elseStmtList;

  public SqlIfStmt(final SqlParserPos pos,
      final SqlNodeList conditionalStmtListPairs,
      final SqlNodeList elseStmtList) {
    super(pos);
    this.conditionalStmtListPairs = conditionalStmtListPairs;
    this.elseStmtList = elseStmtList;
  }

  @Override public SqlOperator getOperator() {
    return null;
  }

  @Override public List<SqlNode> getOperandList() {
    return null;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    for (int i = 0; i < conditionalStmtListPairs.size(); i++) {
      if (i != 0) {
        writer.keyword("ELSE IF");
        conditionalStmtListPairs.get(i).unparse(writer, leftPrec, rightPrec);
      } else {
        writer.keyword("IF");
        conditionalStmtListPairs.get(i).unparse(writer, leftPrec, rightPrec);
      }
      if (!SqlNodeList.isEmptyList(elseStmtList)) {
        writer.keyword("ELSE");
        elseStmtList.unparse(writer, leftPrec, rightPrec);
      }
    }
    writer.keyword("END IF");
  }
}
