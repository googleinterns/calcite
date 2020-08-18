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
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Objects;

public class FunctionParameterImpl implements FunctionParameter {

  private final int ordinal;
  private final String name;
  private final RelDataType type;
  private final boolean optional;

  public FunctionParameterImpl(int ordinal, String name, RelDataType type,
      boolean optional) {
    this.ordinal = ordinal;
    this.name = Objects.requireNonNull(name);
    this.type = Objects.requireNonNull(type);
    this.optional = optional;
  }

  @Override public int getOrdinal() {
    return ordinal;
  }

  @Override public String getName() {
    return name;
  }

  @Override public RelDataType getType(RelDataTypeFactory typeFactory) {
    return type;
  }

  @Override public boolean isOptional() {
    return optional;
  }
}
