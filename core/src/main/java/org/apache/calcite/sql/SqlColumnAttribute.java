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
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/**
 * A <code>SqlColumnAttribute</code> is a base class that can be used
 * to create custom attributes for columns created by the SQL CREATE TABLE
 * function.
 *
 * <p>To customize column attribute unparsing, override the method.</p>
 * {@link #unparse(SqlWriter, int, int)}.
 */
public abstract class SqlColumnAttribute extends SqlNode {

  /**
   * Creates a {@code SqlColumnAttribute}.
   *
   * @param pos  Parser position, must not be null
   */
  protected SqlColumnAttribute(SqlParserPos pos) {
    super(pos);
  }

  @Override public SqlNode clone(final SqlParserPos pos) {
    return null;
  }

  @Override public void validate(final SqlValidator validator,
      final SqlValidatorScope scope) {
  }

  @Override public <R> R accept(final SqlVisitor<R> visitor) {
    return null;
  }

  @Override public boolean equalsDeep(final SqlNode node, final Litmus litmus) {
    return false;
  }
}
