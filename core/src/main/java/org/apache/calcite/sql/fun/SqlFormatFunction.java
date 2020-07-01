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
package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWriter;

/**
   * The SQL <code>FORMAT</code> operator.
   *
   * <p>The SQL syntax is
   *
   * <blockquote><code>FORMAT(<i>expression</i> <i>literal</i>)</code>
   * </blockquote>
   */
public class SqlFormatFunction extends SqlFunction {

  public SqlFormatFunction() {
    super("FORMAT", SqlKind.FORMAT, null, null, null, SqlFunctionCategory.STRING);
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    call.operand(0).unparse(writer, 0, 0);
    final SqlWriter.Frame frame = writer.startList("(", ")");
    writer.keyword(getName());
    call.operand(1).unparse(writer, 0, 0);
    writer.endList(frame);
  }
}
