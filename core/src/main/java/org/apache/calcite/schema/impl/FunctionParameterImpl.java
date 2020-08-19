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

import java.util.Objects;

/**
 * Implementation of {@code FunctionParameter}.
 */
public class FunctionParameterImpl implements FunctionParameter {

  private final int ordinal;
  private final String name;
  public final RelDataType type;
  private final boolean optional;

  /**
   * Creates a {@code FunctionParameterImpl}.
   *
   * @param ordinal The index of the parameter
   * @param name The name of the parameter, must not be null
   * @param type The type of the parameter, must not be null
   * @param optional Whether or not the parameter is optional
   */
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

  @Override public int hashCode() {
    int hashcode = ordinal + type.hashCode();
    if (optional) {
      hashcode++;
    }
    return hashcode;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof FunctionParameterImpl)) {
      return false;
    }
    FunctionParameterImpl other = (FunctionParameterImpl) obj;
    // Name is intentionally not used to allow updating the name of an existing
    // function parameter.
    return ordinal == other.getOrdinal()
        && type.getFullTypeString().equals(other.type.getFullTypeString())
        && optional == other.isOptional();
  }
}
