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
import org.apache.calcite.sql.validate.BlockScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

/**
 * Parse tree for a {@code SqlLeaveStmt}.
 */
public class SqlLeaveStmt extends SqlScriptingNode {
  public static final SqlSpecialOperator OPERATOR =
      new SqlSpecialOperator("LEAVE", SqlKind.LEAVE_STATEMENT);

  public final SqlIdentifier label;
  public SqlLabeledBlock labeledBlock;

  /**
   * Creates a {@code SqlLeaveStmt}.
   *
   * @param pos     Parser position, must not be null.
   * @param label   Labeled statement to be terminated.
   */
  public SqlLeaveStmt(final SqlParserPos pos, final SqlIdentifier label) {
    super(pos);
    this.label = label;
  }

  @Override public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(label);
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    writer.keyword("LEAVE");
    label.unparse(writer, leftPrec, rightPrec);
  }

  @Override public void validate(final SqlValidator validator,
      final SqlValidatorScope scope) {
    BlockScope bs = (BlockScope) scope;
    labeledBlock = bs.findLabeledBlock(label);
  }
}
