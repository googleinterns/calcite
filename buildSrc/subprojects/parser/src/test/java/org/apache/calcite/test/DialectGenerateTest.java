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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DialectGenerateTest {

  private void assertFunctionIsParsed(String declaration, String function) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    int charIndex = dialectGenerate.processFunction(
        DialectGenerate.getTokens(function), new LinkedHashMap<String,String>(),
        0, declaration.length(), "foo");
    assertEquals(function.length(), charIndex);
  }

  private void assertFileProcessed(String testName) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    Path basePath = Paths.get("src", "test", "processFileTests", testName);
    Path testPath = basePath.resolve(testName + ".txt");
    Path expectedPath = basePath.resolve(testName + "_expected.txt");

    String fileText = readFile(testPath);
    Map<String, String> functionMap = new LinkedHashMap<String, String>();
    dialectGenerate.processFile(fileText, functionMap);

    String expectedText = readFile(expectedPath);
    StringBuilder actualText = new StringBuilder();
    for (String value : functionMap.values()) {
      actualText.append(value + "\n");
    }
    assertEquals(expectedText, actualText.toString());
  }

  private String readFile(Path path) {
    String fileText = "";
    try {
      fileText = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    } catch (IOException e) {
      assertTrue(false, "File " + path.toAbsolutePath().toString()
          + " doesn't exist.");
    }
    // Windows line ending converting.
    fileText = fileText.replaceAll("\\r\\n", "\n");
    fileText = fileText.replaceAll("\\r", "\n");
    return fileText;
  }


  @Test public void processFunctionEmptyMultipleLines() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "{\n"
      + "}\n"
      + "{\n"
      + "}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionEmptySameLineWithSpaces() {
    String declaration = "void foo() :";
    String function = declaration + " {} {}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionEmptyLinesBetweenCurlyBlocks() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "\n"
      + "{\n"
      + "}\n"
      + "\n"
      + "{\n"
      + "}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionStringDeclaration() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "{\n"
      + "    String x = \" } \";\n"
      + "}\n"
      + "{\n"
      + "}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionCharacterDeclaration() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "{\n"
      + "    Character x = ' } ';\n"
      + "}\n"
      + "{\n"
      + "}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionContainsComments() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "{\n"
      + "    // }\n"
      + "}\n"
      + "{\n"
      + "    /* } */\n"
      + "}";
    assertFunctionIsParsed(declaration, function);
  }

  @Test public void processFunctionMissingCurlyBlockFails() {
    String declaration = "void foo() :\n";
    String function = declaration
      + "{\n"
      + "}";
    assertThrows(IllegalStateException.class,
        () -> assertFunctionIsParsed(declaration, function));
  }

  @Test public void processFileEmpty() {
    String testName = "empty";
    assertFileProcessed(testName);
  }

  @Test public void processFileSingleFunction() {
    String testName = "single_function";
    assertFileProcessed(testName);
  }

  @Test public void processFileMultiLineDeclarations() {
    String testName = "multi_line_declarations";
    assertFileProcessed(testName);
  }

  @Test public void processFileMultipleFunctionsSeparatedByLines() {
    String testName = "multiple_functions_separated";
    assertFileProcessed(testName);
  }
}
