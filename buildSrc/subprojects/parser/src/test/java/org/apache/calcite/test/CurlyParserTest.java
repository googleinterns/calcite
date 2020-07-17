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

import java.util.Queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.apache.calcite.buildtools.parser.CurlyParser;
import org.apache.calcite.buildtools.parser.DialectGenerate;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CurlyParserTest {

  private void assertCurlyParserSucceeds(String input) {
    CurlyParser parser = new CurlyParser();
    Queue<String> tokens = DialectGenerate.getTokens(input);
    boolean doneParsing = false;
    while (!tokens.isEmpty()) {
      assertFalse(doneParsing, "Failed to parse curly block");
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

  @Test public void testCurlyParserParsesDoubleQuoteInString() {
    String input = "{ \"abc\\\" \"  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesSingleQuoteInSingleQuotes() {
    String input = "{ 'abc\\''  }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserParsesNestedValidCurlyBlocks() {
    String input = "{ {  } }";
    assertCurlyParserSucceeds(input);
  }

  @Test public void testCurlyParserThrowsExceptionLeadingSpaces() {
    String input = " { }";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }

  @Test public void testCurlyParserThrowsExceptionLeadingNewLine() {
    String input = "\n{ }";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }

  @Test public void testCurlyParserThrowsExceptionClosedTooEarly() {
    String input = "{ }}";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }

  @Test public void testCurlyParserThrowsExceptionNeverClosed() {
    String input = "{{}";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }

  @Test public void testCurlyParserThrowsExceptionSingleOpen() {
    String input = "{";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }

  @Test public void testCurlyParserThrowsExceptionSingleCLosed() {
    String input = "}";
    assertThrows(AssertionError.class, () -> assertCurlyParserSucceeds(input));
  }
}
