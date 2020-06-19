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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Arrays;
/**
 * A <code>SqlCharacterSetToCharacterSet</code> is an AST node contains
 * the structure of CharacterSet to CharacterSet token
 */
public class SqlCharacterSetToCharacterSet extends SqlIdentifier {
  /**
   * Creates a {@code SqlCharacterSetToCharacterSet}.
   *
   * @param charSetNamesPrimitiveArr Primitive string array of two character sets
   * @param pos  Parser position, must not be null
   */
  public SqlCharacterSetToCharacterSet(
      final String[] charSetNamesPrimitiveArr,
      final SqlParserPos pos) {
    super(new ArrayList<>(Arrays.asList(charSetNamesPrimitiveArr)), pos);
  }

  @Override public void unparse(final SqlWriter writer,
      final int leftPrec, final int rightPrec) {
    writer.print(names.get(0) + "_TO_" + names.get(1));
  }
}
