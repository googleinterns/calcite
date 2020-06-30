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
import org.junit.jupiter.api.Assertions;
import org.apache.calcite.buildtools.parser.DialectGenerate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DialectGenerateTest {

  /**
   * Returns a DialectGenerate with root path of calcite/parsingTest and
   * dialect path of calcite/parsingTest/intermediate/testDialect.
   */
  private DialectGenerate setupDialectGenerate() {
    Path rootPath = Paths.get("..", "..", "..", "parsingTest");
    // Adds the path /intermediate/testDialect/ to the end of rootPath.
    Path dialectPath = rootPath.resolve(Paths.get("intermediate",
          "testDialect"));
    File rootFile = rootPath.toFile();
    File dialectFile = dialectPath.toFile();
    return new DialectGenerate(dialectFile, rootFile, "");
  }

  /**
   * Returns the expected extraction results after calling
   * DialectGenerate.extractFunctions onto the parsingTest directory structure.
   */
  private Map<String, String> getExpectedExtractionResults() {
    String foo = "void foo() :\n"
      + "{\n"
      + "    String x = \" '}' \";\n"
      + "}\n"
      + "{\n"
      + "    // overidden by testDialect\n"
      + "}";
    String bar = "void bar  (   )  : {} {}";
    String baz = "void baz() : {x}\n"
      + "{\n"
      + "    // overridden by intermediate\n"
      + "}";
    String qux = "void qux(int arg1, int arg2) :\n"
      + "{\n"
      + "    String x = \" } \"\n"
      + "}\n"
      + "{\n"
      + "    // Below is a }\n"
      + "}";
    String quux = "void quux(int arg1,\n"
      + "    int arg2\n"
      + ") :\n"
      + "{\n"
      + "    char x = ' } '\n"
      + "    String y;\n"
      + "}\n"
      + "{\n"
      + "    /* All invalid curly braces:\n"
      + "    }\n"
      + "    // }\n"
      + "    \" } \"\n"
      + "    ' } '\n"
      + "    */\n"
      + "    <TOKEN> {\n"
      + "        y = \" { } } \"\n"
      + "    }\n\n"
      + "    // Not a string: \"\n"
      + "}";
    return new LinkedHashMap<String, String>() {{
      put("foo", foo);
      put("bar", bar);
      put("baz", baz);
      put("qux", qux);
      put("quux", quux);
    }};
  }

  @Test public void testExtractFunctions() {
    DialectGenerate dialectGenerate = setupDialectGenerate();
    Map<String, String> result = dialectGenerate.extractFunctions();
    Map<String, String> expected = getExpectedExtractionResults();
    assertEquals(expected.size(), result.size(),
        "Resultant map size doesn't match expected map size");
    for (String key : expected.keySet()) {
      assertTrue(result.containsKey(key),
          "Resultant map doesn't contain expected key: " + key);
      assertEquals(expected.get(key), result.get(key));
    }
  }
}
