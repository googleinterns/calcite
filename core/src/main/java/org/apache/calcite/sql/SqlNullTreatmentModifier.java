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

import com.google.common.base.Preconditions;

import java.util.Objects;

/** Simple class to hold null treatment info. */
public class SqlNullTreatmentModifier extends SqlNode {
  public final SqlKind kind;

  public SqlNullTreatmentModifier(SqlParserPos pos, SqlKind kind) {
    super(pos);
    this.kind = Objects.requireNonNull(kind);
    Preconditions.checkArgument(kind == SqlKind.RESPECT_NULLS
        || kind == SqlKind.IGNORE_NULLS);
  }

  @Override public SqlNode clone(SqlParserPos pos) {
    return null;
  }

  @Override public void validate(
      SqlValidator validator,
      SqlValidatorScope scope) {
  }

  @Override public <R> R accept(SqlVisitor<R> visitor) {
    throw new UnsupportedOperationException();
  }

  @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
    return false;
  }

  @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    if (kind == SqlKind.RESPECT_NULLS) {
      writer.keyword("RESPECT NULLS");
    } else {
      writer.keyword("IGNORE NULLS");
    }
  }
}
