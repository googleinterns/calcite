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

import java.util.Collection;
import java.util.Objects;

/**
 * A <code>SqlStatementList</code> is a list of {@link SqlNode}s that are SQL
 * statements with a terminating semicolon. It is also a {@link SqlNode}, so
 * it may appear in a parse tree.
 */
public class SqlStatementList extends SqlNodeList {

  public SqlStatementList(SqlParserPos pos) {
    super(pos);
  }

  public SqlStatementList(Collection<? extends SqlNode> collection,
      SqlParserPos pos) {
    super(collection, pos);
  }

  @Override public void add(final SqlNode node) {
    super.add(Objects.requireNonNull(node));
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec,
      final int rightPrec) {
    SqlWriter.Frame frame = writer.startList(
        SqlWriter.FrameTypeEnum.STATEMENT_LIST, "", "");
    for (SqlNode e : getList()) {
      e.unparse(writer, 0, 0);
      writer.setNeedWhitespace(false);
      writer.sep(";");
      writer.newlineAndIndent();
    }
    writer.endList(frame);
  }
}
