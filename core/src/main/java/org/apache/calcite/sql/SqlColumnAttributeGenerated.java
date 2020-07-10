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

import java.util.List;
import java.util.Objects;

/**
 * A {@code SqlColumnAttributeGenerated} represents a column
 * attribute specified by the GENERATED statement.
 */
public class SqlColumnAttributeGenerated extends SqlColumnAttribute {

  public final GeneratedType generatedType;
  public final List<SqlColumnAttributeGeneratedOption> generateOptions;

  public enum GeneratedType {
    ALWAYS,
    BY_DEFAULT
  }

  /**
   * Creates a {@code SqlColumnAttributeGenerated}.
   *
   * @param pos               Parser position.
   * @param generatedType     Enum representing whether ALWAYS or BY DEFAULT
   *                            was specified.
   * @param generateOptions   List of options for GENERATED statement
   *                            (ex. START WITH, MINVALUE).
   */
  public SqlColumnAttributeGenerated(SqlParserPos pos,
      GeneratedType generatedType,
      List<SqlColumnAttributeGeneratedOption> generateOptions) {
    super(pos);
    this.generatedType = Objects.requireNonNull(generatedType);
    this.generateOptions = Objects.requireNonNull(generateOptions);
  }

  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("GENERATED");
    switch (generatedType) {
    case ALWAYS:
      writer.keyword("ALWAYS");
      break;
    case BY_DEFAULT:
      writer.keyword("BY DEFAULT");
      break;
    default:
      break;
    }
    writer.keyword("AS IDENTITY");
    if (generateOptions.isEmpty()) {
      return;
    }
    SqlWriter.Frame frame = writer.startList("(", ")");
    for (SqlColumnAttributeGeneratedOption c : generateOptions) {
      c.unparse(writer, 0, 0);
    }
    writer.endList(frame);
  }
}
