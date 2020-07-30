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
package org.apache.calcite.test;

import java.nio.file.Path;
import java.io.File;
import java.nio.file.Paths;
import java.util.Queue;
import java.util.LinkedList;
import java.lang.StringBuilder;
import java.util.Arrays;
import java.util.Map;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.Test;
import org.apache.calcite.buildtools.parser.DialectTraverser;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DialectTraverserTest {

  private static final Path outputPath = Paths.get("build", "integrationTest",
      "actual.ftl");
  private static final Path rootPath = Paths.get("src", "test", "resources",
      "parserTest");

  /**
   * Returns a DialectTraverser with root path of calcite/parsingTest and
   * dialect path of calcite/parsingTest/intermediate/testDialect.
   */
  private DialectTraverser setupDialectTraverser() {
    // Adds the path /intermediate/dialects/testDialect/ to the end of rootPath.
    Path dialectPath = rootPath.resolve(Paths.get("intermediate", "dialects",
          "testDialect"));
    File rootFile = rootPath.toFile();
    File dialectFile = dialectPath.toFile();
    return new DialectTraverser(dialectFile, rootFile, outputPath.toString());
  }

  /*@Test public void testExtractionGenerationOnTestDirectory() {
    DialectTraverser dialectTraverser = setupDialectTraverser();
    dialectTraverser.run();
    String actualText = TestUtils.readFile(rootPath.resolve(
          Paths.get("intermediate", "dialects", "testDialect")
          .resolve(outputPath)));
    String expectedText = TestUtils.readFile(Paths.get("src", "test",
          "resources", "integrationTest", "expected.ftl"));
    assertEquals(expectedText, actualText);
  }*/
}
