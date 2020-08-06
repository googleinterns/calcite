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

import java.lang.StringBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.apache.calcite.buildtools.parser.DialectGenerate;
import org.apache.calcite.buildtools.parser.ExtractedData;
import org.apache.calcite.buildtools.parser.Keyword;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DialectGenerateTest {

  private void assertFunctionIsParsed(String declaration, String function) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    int charIndex = dialectGenerate.processFunction(
        DialectGenerate.getTokens(function), new LinkedHashMap<String,String>(),
        0, declaration.length(), "foo", new StringBuilder());
    assertEquals(function.length(), charIndex);
  }

  private void assertTokenAssignmentIsParsed(String declaration, String function) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    int charIndex = dialectGenerate.processTokenAssignment(
        DialectGenerate.getTokens(function), new LinkedList<String>(),
        0, declaration.length(), new StringBuilder());
    assertEquals(function.length(), charIndex);
  }

  private void assertFunctionNameExtracted(String declaration, String name) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    assertEquals(name, dialectGenerate.getFunctionName(declaration));
  }

  private void assertNonReservedKeywordsValid(ExtractedData extractedData) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    assertDoesNotThrow(() ->
        dialectGenerate.validateNonReservedKeywords(extractedData));
  }

  private void assertNonReservedKeywordsInvalid(ExtractedData extractedData) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    assertThrows(IllegalStateException.class, () ->
        dialectGenerate.validateNonReservedKeywords(extractedData));
  }

  /**
   * Each testcase has a testName.txt and testName_expected.txt file.
   * This function makes sure that the testName.txt file is parsed without error
   * by checking that the contents of the modified functionMap match the contents
   * of the testName_expected.txt file.
   */
  private void assertFileProcessed(String testName, boolean specifyFilePath) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    Path resourcePath = Paths.get("src", "test", "resources");
    Path basePath = resourcePath.resolve(Paths.get("processFileTests",
          testName));

    Path testPath = basePath.resolve(testName + ".txt");
    Path expectedPath = basePath.resolve(testName + "_expected.txt");
    Path licensePath = resourcePath.resolve(Paths.get("parserTest",
        "src", "resources", "license.txt"));

    String fileText = TestUtils.readFile(testPath);
    ExtractedData extractedData = new ExtractedData();
    String filePath = null;
    if (specifyFilePath) {
      // For windows paths change separator to forward slash.
      filePath = Paths.get("processFileTests", testName, testName +".txt").toString();
      filePath = filePath.replace("\\", "/");
    }
    dialectGenerate.processFile(fileText, extractedData, filePath);
    String expectedText = TestUtils.readFile(expectedPath);
    String licenseText = TestUtils.readFile(licensePath);
    StringBuilder actualText = new StringBuilder();
    actualText.append(licenseText);
    for (String value : extractedData.tokenAssignments) {
      actualText.append("\n").append(value).append("\n");
    }
    for (String value : extractedData.functions.values()) {
      actualText.append("\n").append(value).append("\n");
    }
    assertEquals(expectedText, actualText.toString());
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
    assertFileProcessed("empty", /*specifyFilePath=*/ true);
  }

  @Test public void processFileSingleFunctionFilePathNotSpecified() {
    assertFileProcessed("single_function", /*specifyFilePath=*/ false);
  }

  @Test public void processFileMultiLineDeclarations() {
    assertFileProcessed("multi_line_declarations", /*specifyFilePath=*/ true);
  }

  @Test public void processFileMultipleFunctionsSeparatedByLines() {
    assertFileProcessed("multiple_functions_separated", /*specifyFilePath=*/ true);
  }

  @Test public void processFileTokenAssignments() {
    assertFileProcessed("token_assignments", /*specifyFilePath=*/ true);
  }

  @Test public void processFileFunctionsAndTokenAssignments() {
    assertFileProcessed("functions_and_assignments", /*specifyFilePath=*/ true);
  }

  @Test public void processFileTypesWithAngleBrackets() {
    assertFileProcessed("angle_brackets", /*specifyFilePath=*/ true);
  }

  @Test public void processTokenAssignmentTokenEmpty() {
    String declaration = "TOKEN :\n";
    String assignment = declaration
      + "{\n}";
    assertTokenAssignmentIsParsed(declaration, assignment);
  }

  @Test public void processTokenAssignmentSkipEmpty() {
    String declaration = "SKIP :\n";
    String assignment = declaration
      + "{\n}";
    assertTokenAssignmentIsParsed(declaration, assignment);
  }

  @Test public void processTokenAssignmentMoreEmpty() {
    String declaration = "MORE :\n";
    String assignment = declaration
      + "{\n}";
    assertTokenAssignmentIsParsed(declaration, assignment);
  }

  @Test public void processTokenAssignmentNonEmptyWithAngleBrackets() {
    String declaration = "<foo, bar> TOKEN :\n";
    String assignment = declaration
      + "{\n"
      + "< DATE_PART: \"DATE_PART\" >\n"
      + "< NEGATE: \"!\" >\n"
      + "}";
    assertTokenAssignmentIsParsed(declaration, assignment);
  }

  @Test public void getFunctionNameSingleWord() {
    String declaration = "String foo () :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void getFunctionNameSingleAngleBrackets() {
    String declaration = "List<String> foo () :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void getFunctionNameNestedAngleBrackets() {
    String declaration = "List<List<String>> foo () :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void getFunctionNameSingleAngleBracketsMultipleOptions() {
    String declaration = "Map<String, String> foo () :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void getFunctionNameWithArguments() {
    String declaration = "Map<String, String> foo(String x, int y) :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void getFunctionNameWithDot() {
    String declaration = "Foo.Bar baz() :";
    assertFunctionNameExtracted(declaration, "baz");
  }

  @Test public void getFunctionNameWithFinal() {
    String declaration = "final void foo() :";
    assertFunctionNameExtracted(declaration, "foo");
  }

  @Test public void testNonReservedKeywordsValidEmpty() {
    assertNonReservedKeywordsValid(new ExtractedData());
  }

  @Test public void testNonReservedKeywordsValidNonEmpty() {
    ExtractedData extractedData = new ExtractedData();
    extractedData.keywords.put("foo", new Keyword("foo"));
    extractedData.keywords.put("bar", new Keyword("bar"));
    extractedData.nonReservedKeywords.put("bar", new Keyword("bar"));
    assertNonReservedKeywordsValid(extractedData);
  }

  @Test public void testProcessKeywordsInvalidNonReservedKeywordFails() {
    ExtractedData extractedData = new ExtractedData();
    extractedData.keywords.put("foo", new Keyword("foo"));
    extractedData.nonReservedKeywords.put("bar", new Keyword("bar"));
    assertNonReservedKeywordsInvalid(extractedData);
  }

  @Test public void testUnparseTokenAssignmentEmpty() {
    Map<String, Keyword> keywords = new LinkedHashMap<>();
    DialectGenerate dialectGenerate = new DialectGenerate();
    String actual = dialectGenerate.unparseTokenAssignment(keywords);
    assertEquals("", actual);
  }

  @Test public void testUnparseTokenAssignmentSingle() {
    Map<String, Keyword> keywords = new LinkedHashMap<>();
    DialectGenerate dialectGenerate = new DialectGenerate();
    keywords.put("foo", new Keyword("foo", "\"FOO\"", "path/file"));
    String actual = dialectGenerate.unparseTokenAssignment(keywords);
    String expected = "// Auto generated.\n"
         + "<DEFAULT, DQID, BTID> TOKEN :\n"
         + "{\n"
         + "< FOO: \"FOO\" > // From: path/file\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testUnparseTokenAssignmentMultiple() {
    Map<String, Keyword> keywords = new LinkedHashMap<>();
    DialectGenerate dialectGenerate = new DialectGenerate();
    keywords.put("foo", new Keyword("foo", "\"FOO\"", "path/file"));
    keywords.put("bar", new Keyword("bar", "\"BAR\""));
    keywords.put("baz", new Keyword("baz", "\"BAZ\"", "path/file"));
    String actual = dialectGenerate.unparseTokenAssignment(keywords);
    String expected = "// Auto generated.\n"
         + "<DEFAULT, DQID, BTID> TOKEN :\n"
         + "{\n"
         + "< FOO: \"FOO\" > // From: path/file\n"
         + "| < BAR: \"BAR\" > // No file specified.\n"
         + "| < BAZ: \"BAZ\" > // From: path/file\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testPartitionFunctionSingle() {
    Map<String, Keyword> keywords = new LinkedHashMap<>();
    DialectGenerate dialectGenerate = new DialectGenerate();
    keywords.put("foo", new Keyword("foo", "foo", "path/file"));
    String actual = dialectGenerate.getPartitionFunction("void func():",
        new LinkedHashSet<>(keywords.values()));
    String expected = "// Auto generated.\n"
         + "void func():\n"
         + "{\n}\n{\n"
         + "<FOO> // From: path/file\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testPartitionFunctionMultiple() {
    Map<String, Keyword> keywords = new LinkedHashMap<>();
    DialectGenerate dialectGenerate = new DialectGenerate();
    keywords.put("foo", new Keyword("foo", "foo", "path/file"));
    keywords.put("bar", new Keyword("bar", "bar"));
    keywords.put("baz", new Keyword("baz", "baz", "intermediate/path/file"));
    String actual = dialectGenerate.getPartitionFunction("void func():",
       new LinkedHashSet<>(keywords.values()));
    String expected = "// Auto generated.\n"
         + "void func():\n"
         + "{\n}\n{\n"
         + "<FOO> // From: path/file\n"
         + "| <BAR> // No file specified.\n"
         + "| <BAZ> // From: intermediate/path/file\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testUnparseNonReservedKeywordsEmpty() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    dialectGenerate.unparseNonReservedKeywords(extractedData);
    assertEquals(1, extractedData.functions.size());
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword"));
    String actual = extractedData.functions.get("NonReservedKeyword");
    String expected = "// Auto generated.\n"
         + "String NonReservedKeyword():\n"
         + "{\n}\n{\n"
         + "<EOF>\n"
         + "{ return unquotedIdentifier(); }\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testUnparseNonReservedKeywordsSinglePartition() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    extractedData.nonReservedKeywords.put("foo", new Keyword("foo"));
    dialectGenerate.unparseNonReservedKeywords(extractedData);
    assertEquals(2, extractedData.functions.size());
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword"));
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword0of1"));
    String actual = extractedData.functions.get("NonReservedKeyword");
    String expected = "// Auto generated.\n"
         + "String NonReservedKeyword():\n"
         + "{\n}\n{\n"
         + "(\n"
         + "NonReservedKeyword0of1()\n"
         + ")\n"
         + "{ return unquotedIdentifier(); }\n"
         + "}";
    assertEquals(expected, actual);
  }

  @Test public void testUnparseNonReservedKeywordsMultiplePartitions() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    for (int i = 0; i < DialectGenerate.PARTITION_SIZE + 1; i++) {
      String key = "foo" + i;
      extractedData.nonReservedKeywords.put(key, new Keyword(key));
    }
    dialectGenerate.unparseNonReservedKeywords(extractedData);
    assertEquals(3, extractedData.functions.size());
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword"));
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword0of2"));
    assertTrue(extractedData.functions.containsKey("NonReservedKeyword1of2"));
    String actual = extractedData.functions.get("NonReservedKeyword");
    String expected = "// Auto generated.\n"
         + "String NonReservedKeyword():\n"
         + "{\n}\n{\n"
         + "(\n"
         + "NonReservedKeyword0of2()\n"
         + "| NonReservedKeyword1of2()\n"
         + ")\n"
         + "{ return unquotedIdentifier(); }\n"
         + "}";
    assertEquals(expected, actual);
  }
}
