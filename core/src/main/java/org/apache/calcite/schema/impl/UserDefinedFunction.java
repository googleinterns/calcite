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
package org.apache.calcite.schema.impl;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ScalarFunction;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of {@code ScalarFunction} used for functions created using
 * CREATE FUNCTION statements.
 */
public class UserDefinedFunction implements ScalarFunction {

  private final List<FunctionParameter> parameters;
  private final RelDataType returnType;

  /**
   * Creates a {@code UserDefinedFunction}.
   *
   * @param parameters The parameters of the created function
   * @param returnType The return type of the created function
   */
  public UserDefinedFunction(List<FunctionParameter> parameters,
      RelDataType returnType) {
    this.parameters = Objects.requireNonNull(parameters);
    this.returnType = Objects.requireNonNull(returnType);
  }

  @Override public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return returnType;
  }

  @Override public List<FunctionParameter> getParameters() {
    return parameters;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof UserDefinedFunction)) {
      return false;
    }
    UserDefinedFunction other = (UserDefinedFunction) obj;
    // Return type excluded intentionally. This is to allow updating the return
    // type of existing functions.
    return parameters.equals(other.getParameters());
  }
}
