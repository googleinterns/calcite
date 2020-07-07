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

/**
 * A {@code SqlColumnAttributeGeneratedMaxValue} represents the MAXVALUE
 * option of a GENERATED column attribute.
 */
public class SqlColumnAttributeGeneratedMaxValue extends
    SqlColumnAttributeGeneratedOption {

  public final SqlLiteral max;
  public final boolean none;

  /**
   * Creates a {@code SqlColumnAttributeGeneratedMaxValue}.
   *
   * @param max     The amount specified in the MAXVALUE attribute. This
   *                  parameter should only be null when {@code none} is true.
   * @param none    Whether NO MAXVALUE was specified.
   */
  public SqlColumnAttributeGeneratedMaxValue(SqlLiteral max, boolean none) {
    this.max = max;
    this.none = none;
  }

  @Override public void unparse(SqlWriter writer,
      int leftPrec, int rightPrec) {
    if (none) {
      writer.keyword("NO MAXVALUE");
    } else if (max != null) {
      writer.keyword("MAXVALUE");
      max.unparse(writer, leftPrec, rightPrec);
    }
  }
}
