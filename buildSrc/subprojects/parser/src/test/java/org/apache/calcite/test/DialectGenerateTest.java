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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Assertions;
import org.apache.calcite.buildtools.parser.DialectGenerate;
import org.apache.calcite.buildtools.parser.DialectGenerate.CurlyParser;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DialectGenerateTest {

  private DialectGenerate setupDialectGenerate() {
    Path currentRelativePath = Paths.get("");
    String s = currentRelativePath.toAbsolutePath().toString();
    Path rootPath = Paths.get("../../../parsingTest");
    Path dialectPath = Paths.get(rootPath.toAbsolutePath().toString() + "/intermediate/testDialect/");
    File rootFile = rootPath.toFile();
    File dialectFile = dialectPath.toFile();
    return new DialectGenerate(dialectFile, rootFile, "");
  }

  /**
   * Returns the expected extraction results after calling DialectGenerate.extractFunctions
   * onto the parsingTest directory structure.
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

  private Queue<String> getTokens(String str) {
    return new LinkedList(Arrays.asList(DialectGenerate.tokenizerPattern.split(str)));
  }

  private void assertCurlyParserSucceeds(String input) {
    CurlyParser parser = new CurlyParser();
    Queue<String> tokens = getTokens(input);
    boolean doneParsing = false;
    while (!tokens.isEmpty()) {
      if (doneParsing) {
        assertTrue(false, "Failed to parse curly block");
      }
      doneParsing = parser.parseToken(tokens.poll());
    }
    assertTrue(doneParsing);
  }

  @Test public void testCurlyParserParsesEmpty() {
    String input = "{  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesString() {
    String input = "{ \" } \"  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesCharacter() {
    String input = "{ ' } '  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesSingleComment() {
    String input = "{ //  } \n  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesMultiComment() {
    String input = "{ /*  } */  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testExtractFunctions() {
    DialectGenerate dialectGenerate = setupDialectGenerate();
    Map<String, String> res = dialectGenerate.extractFunctions();
    Map<String, String> expected = getExpectedExtractionResults();
    assertEquals(res.size(), expected.size(),
        "Resultant map size doesn't match expected map size");
    for (String key : expected.keySet()) {
      assertTrue(res.containsKey(key), "Resultant map doesn't contain expected key: " + key);
      assertEquals(expected.get(key), res.get(key));
    }
  }
}
