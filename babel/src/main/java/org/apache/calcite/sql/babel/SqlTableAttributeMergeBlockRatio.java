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

import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * A <code>SqlTableAttributeMergeBlockRatio</code> is a table option
 * for the MERGEBLOCKRATIO attribute.
 */
public class SqlTableAttributeMergeBlockRatio extends SqlTableAttribute {

  private final MergeBlockRatioModifier modifier;
  private final int ratio;
  private final boolean percent;

  /**
   * Creates a {@code SqlTableAttributeMergeBlockRatio}.
   *
   * @param modifier  Type of merge block ratio to be used
   * @param ratio  The merge block ratio
   * @param percent  Indicates that the integer value is a percentage
   * @param pos  Parser position, must not be null
   */
  public SqlTableAttributeMergeBlockRatio(MergeBlockRatioModifier modifier,
      int ratio, boolean percent, SqlParserPos pos) {
    super(pos);
    this.modifier = modifier;
    this.ratio = ratio;
    this.percent = percent;
  }

  @Override public void unparse(final SqlWriter writer, final int leftPrec, final int rightPrec) {
    switch (modifier) {
    case UNSPECIFIED:
      writer.keyword("MERGEBLOCKRATIO");
      writer.sep("=");
      writer.print(ratio);
      writer.print(" ");
      if (percent) {
        writer.keyword("PERCENT");
      }
      break;
    case DEFAULT:
      writer.keyword("DEFAULT");
      writer.keyword("MERGEBLOCKRATIO");
      break;
    case NO:
      writer.keyword("NO");
      writer.keyword("MERGEBLOCKRATIO");
      break;
    }
  }

  public enum MergeBlockRatioModifier {
    /**
     * Uses value specified by integer as merge block ratio.
     */
    UNSPECIFIED,

    /**
     * Uses the default value for merge block ratio.
     */
    DEFAULT,

    /**
     * Does not merge small data blocks.
     */
    NO,
  }
}
