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
        0, declaration.length(), "foo");
    assertEquals(function.length(), charIndex);
  }

  private void assertTokenAssignmentIsParsed(String declaration, String function) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    int charIndex = dialectGenerate.processTokenAssignment(
        DialectGenerate.getTokens(function), new LinkedList<String>(),
        0, declaration.length());
    assertEquals(function.length(), charIndex);
  }

  private void assertFunctionNameExtracted(String declaration, String name) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    assertEquals(name, dialectGenerate.getFunctionName(declaration));
  }

  private void assertKeywordsProcessed(Map<Keyword, String> keywords,
      Set<Keyword> nonReservedKeywords, ExtractedData extractedData,
      int keywordsSize, int nonReservedKeywordsSize) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    dialectGenerate.processKeywords(keywords, nonReservedKeywords,
        extractedData);
    assertEquals(extractedData.keywords.size(), keywordsSize);
    assertEquals(extractedData.nonReservedKeywords.size(),
        nonReservedKeywordsSize);
    for (Keyword keyword : keywords.keySet()) {
      assertTrue(extractedData.keywords.containsKey(keyword));
      assertEquals(keywords.get(keyword), extractedData.keywords.get(keyword));
    }
    for (Keyword keyword : nonReservedKeywords) {
      assertTrue(extractedData.nonReservedKeywords.contains(keyword));
    }
  }

  private void assertKeywordsNotProcessed(Map<Keyword, String> keywords,
      Set<Keyword> nonReservedKeywords, ExtractedData extractedData) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    assertThrows(IllegalStateException.class, () -> dialectGenerate
        .processKeywords(keywords, nonReservedKeywords,
        extractedData));
  }

  /**
   * Each testcase has a testName.txt and testName_expected.txt file.
   * This function makes sure that the testName.txt file is parsed without error
   * by checking that the contents of the modified functionMap match the contents
   * of the testName_expected.txt file.
   */
  private void assertFileProcessed(String testName) {
    DialectGenerate dialectGenerate = new DialectGenerate();
    Path resourcePath = Paths.get("src", "test", "resources");
    Path basePath = resourcePath.resolve(Paths.get("processFileTests", testName));

    Path testPath = basePath.resolve(testName + ".txt");
    Path expectedPath = basePath.resolve(testName + "_expected.txt");
    Path licensePath = resourcePath.resolve(Paths.get("parserTest",
        "src", "resources", "license.txt"));

    String fileText = TestUtils.readFile(testPath);
    ExtractedData extractedData = new ExtractedData();
    dialectGenerate.processFile(fileText, extractedData);

    String expectedText = TestUtils.readFile(expectedPath);
    String licenseText = TestUtils.readFile(licensePath);
    StringBuilder actualText = new StringBuilder();
    actualText.append(licenseText);
    for (String value : extractedData.tokenAssignments) {
      actualText.append(value + "\n");
    }
    for (String value : extractedData.functions.values()) {
      actualText.append(value + "\n");
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
    assertFileProcessed("empty");
  }

  @Test public void processFileSingleFunction() {
    assertFileProcessed("single_function");
  }

  @Test public void processFileMultiLineDeclarations() {
    assertFileProcessed("multi_line_declarations");
  }

  @Test public void processFileMultipleFunctionsSeparatedByLines() {
    assertFileProcessed("multiple_functions_separated");
  }

  @Test public void processFileTokenAssignments() {
    assertFileProcessed("token_assignments");
  }

  @Test public void processFileFunctionsAndTokenAssignments() {
    assertFileProcessed("functions_and_assignments");
  }

  @Test public void processFileTypesWithAngleBrackets() {
    assertFileProcessed("angle_brackets");
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

  @Test public void testProcessKeywordsBothEmpty() {
    int keywordsSize = 0;
    int nonReservedKeywordsSize = 0;
    assertKeywordsProcessed(new LinkedHashMap<Keyword, String>(),
        new HashSet<Keyword>(), new ExtractedData(), keywordsSize,
        nonReservedKeywordsSize);
  }

  @Test public void testProcessKeywordsBothNonEmptyAndValid() {
    int keywordsSize = 2;
    int nonReservedKeywordsSize = 1;
    Map<Keyword, String> keywords = new LinkedHashMap<Keyword, String>();
    Set<Keyword> nonReservedKeywords = new HashSet<Keyword>();

    keywords.put(new Keyword("foo"), "foo");
    keywords.put(new Keyword("bar"), "bar");
    nonReservedKeywords.add(new Keyword("bar"));
    assertKeywordsProcessed(keywords, nonReservedKeywords, new ExtractedData(),
        keywordsSize, nonReservedKeywordsSize);
  }

  @Test public void testProcessKeywordsNonReservedKeywordInExtractedData() {
    int keywordsSize = 2;
    int nonReservedKeywordsSize = 1;
    Map<Keyword, String> keywords = new LinkedHashMap<Keyword, String>();
    ExtractedData extractedData = new ExtractedData();
    Set<Keyword> nonReservedKeywords = new HashSet<Keyword>();

    keywords.put(new Keyword("foo"), "foo");
    extractedData.keywords.put(new Keyword("bar"), "bar");
    nonReservedKeywords.add(new Keyword("bar"));
    assertKeywordsProcessed(keywords, nonReservedKeywords, extractedData,
        keywordsSize, nonReservedKeywordsSize);
  }

  @Test public void testProcessKeywordsKeywordGetsOverridden() {
    int keywordsSize = 1;
    int nonReservedKeywordsSize = 0;
    Map<Keyword, String> keywords = new LinkedHashMap<Keyword, String>();
    ExtractedData extractedData = new ExtractedData();
    Set<Keyword> nonReservedKeywords = new HashSet<Keyword>();
    extractedData.keywords.put(new Keyword("foo"), "foo1");
    keywords.put(new Keyword("foo"), "foo2");
    assertKeywordsProcessed(keywords, nonReservedKeywords, extractedData,
        keywordsSize, nonReservedKeywordsSize);
  }

  @Test public void testProcessKeywordsInvalidNonReservedKeywordFails() {
    Map<Keyword, String> keywords = new LinkedHashMap<Keyword, String>();
    Set<Keyword> nonReservedKeywords = new HashSet<Keyword>();

    keywords.put(new Keyword("foo"), "foo");
    nonReservedKeywords.add(new Keyword("bar"));
    assertKeywordsNotProcessed(keywords, nonReservedKeywords,
        new ExtractedData());
  }

  @Test public void testUnparseReservedKeywordsEmpty() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    dialectGenerate.unparseReservedKeywords(extractedData);
    assertEquals(0, extractedData.tokenAssignments.size());
  }

  @Test public void testUnparseReservedKeywordsSingle() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    extractedData.keywords.put(new Keyword("foo", "path/file"), "FOO");
    dialectGenerate.unparseReservedKeywords(extractedData);

    String actual = extractedData.tokenAssignments.get(0);
    String expected = "<DEFAULT, DQID, BTID> TOKEN :\n"
         + "{\n"
         + "<FOO : \"FOO\"> // From: path/file\n"
         + "}\n";
    assertEquals(1, extractedData.tokenAssignments.size());
    assertEquals(expected, actual);
  }

  @Test public void testUnparseReservedKeywordsMultiple() {
    ExtractedData extractedData = new ExtractedData();
    DialectGenerate dialectGenerate = new DialectGenerate();
    extractedData.keywords.put(new Keyword("foo", "path/file"), "FOO");
    extractedData.keywords.put(new Keyword("bar", /*filePath=*/ null), "BAR");
    extractedData.keywords.put(new Keyword("baz", "path/file"), "BAZ");
    dialectGenerate.unparseReservedKeywords(extractedData);

    String actual = extractedData.tokenAssignments.get(0);
    String expected = "<DEFAULT, DQID, BTID> TOKEN :\n"
         + "{\n"
         + "<FOO : \"FOO\"> // From: path/file\n"
         + "| <BAR : \"BAR\"> // No file specified.\n"
         + "| <BAZ : \"BAZ\"> // From: path/file\n"
         + "}\n";
    assertEquals(1, extractedData.tokenAssignments.size());
    assertEquals(expected, actual);
  }
}
