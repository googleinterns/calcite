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
package com.apache.calcite.parsing.tools;

import java.io.IOException;
import java.util.Locale;

final class FindDialectParsingErrorsExec {

  private static final int MAX_NUM_SAMPLE_QUERIES = 20;
  private static final int DEFAULT_NUM_SAMPLE_QUERIES = 5;

  private FindDialectParsingErrorsExec() {
    // Needs to be private to fix HideUtilityClassConstructor error for
    // checkstyle
  }

  public static void main(String[] args) throws IOException {
    String inputPath = args[0];
    String outputPath = args[1];
    FindDialectParsingErrors.Dialect dialect =
        FindDialectParsingErrors.Dialect.valueOf(args[2]
        .toUpperCase(Locale.ROOT));
    boolean groupByErrors = Boolean.parseBoolean(args[3]);
    int numSampleQueries = DEFAULT_NUM_SAMPLE_QUERIES;
    if (args.length == 5) {
      numSampleQueries = Integer.parseInt(args[4]);
      if (numSampleQueries < 1 || numSampleQueries > MAX_NUM_SAMPLE_QUERIES) {
        throw new IllegalArgumentException("numSampleQueries must be between 1 "
            + "and " + MAX_NUM_SAMPLE_QUERIES);
      }
    }
    FindDialectParsingErrors findDialectParsingErrors =
        new FindDialectParsingErrors(inputPath, outputPath, dialect,
            groupByErrors, numSampleQueries);
    findDialectParsingErrors.run();
  }
}
