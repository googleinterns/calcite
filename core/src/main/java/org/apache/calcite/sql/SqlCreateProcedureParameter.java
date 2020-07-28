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

public class SqlCreateProcedureParameter {

  public final CreateProcedureParameterType parameterType;
  public final SqlIdentifier name;
  public final SqlDataTypeSpec dataTypeSpec;

  public SqlCreateProcedureParameter(CreateProcedureParameterType parameterType,
      SqlIdentifier name, SqlDataTypeSpec dataTypeSpec) {
    this.parameterType = parameterType;
    this.name = name;
    this.dataTypeSpec = dataTypeSpec;
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(parameterType.toString());
    name.unparse(writer, leftPrec, rightPrec);
    dataTypeSpec.unparse(writer, leftPrec, rightPrec);
  }

  public enum CreateProcedureParameterType {
    /**
     * This is an input parameter. This is the default parameter type if none is
     * specified.
     */
    IN,

    /**
     * This is an output parameter.
     */
    OUT,

    /**
     * This parameter can be input and output.
     */
    INOUT,
  }
}
