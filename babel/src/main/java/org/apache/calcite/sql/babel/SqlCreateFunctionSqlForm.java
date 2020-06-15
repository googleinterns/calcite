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

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlCreateFunctionSqlForm extends org.apache.calcite.sql.ddl.SqlCreateFunction {
  public final SqlDataTypeSpec returnsDataType;
  public final boolean isDeterministic;
  public final boolean canRunOnNullInput;
  public final boolean hasSqlSecurityDefiner;
  public final int typeInt;
  public final SqlNode returnExpression;

  /** Creates a SqlCreateFunctionSqlForm.
   * @param pos position
   * @param replace if "or replace" token occurred
   * @param ifNotExists if "not exists" token occurred
   * @param name the name of the function
   * @param className ?
   * @param usingList list of parameter name and type
   * @param returnsDataType return type of the function
   * @param isDeterministic if "deterministic" is specified
   * @param canRunOnNullInput if "called on null input" or "returns null on null input"
   *                          is specified
   * @param hasSqlSecurityDefiner if "sql security definer" is specified
   * @param returnExpression the expression that is returned
   * */
  public SqlCreateFunctionSqlForm(final SqlParserPos pos, final boolean replace,
      final boolean ifNotExists,
      final SqlIdentifier name, final SqlNode className,
      final SqlNodeList usingList, final SqlDataTypeSpec returnsDataType,
      final boolean isDeterministic, final boolean canRunOnNullInput,
      final boolean hasSqlSecurityDefiner, final int typeInt, final SqlNode returnExpression) {

    super(pos, replace, ifNotExists, name, className, usingList);

    this.returnsDataType = returnsDataType;
    this.isDeterministic = isDeterministic;
    this.canRunOnNullInput = canRunOnNullInput;
    this.hasSqlSecurityDefiner = hasSqlSecurityDefiner;
    this.typeInt = typeInt;
    this.returnExpression = returnExpression;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE FUNCTION");
    name.unparse(writer, 0,  0);
    writer.print("() ");
    writer.keyword("RETURNS");
    returnsDataType.unparse(writer, 0, 0);
    writer.keyword("LANGUAGE SQL COLLATION INVOKER INLINE TYPE");
    writer.print(typeInt + " ");
    writer.keyword("RETURN");
    returnExpression.unparse(writer, 0, 0);
  }
}
