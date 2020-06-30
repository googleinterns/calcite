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
package org.apache.calcite.buildtools.parser;

import java.lang.StringBuilder;
import java.lang.IllegalStateException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.LinkedList;
import java.util.LinkedHashMap;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;

/**
 * Contains the logic to extract functions and generate the parserImpls.ftl file
 * for the given dialect. Used by the DialectGenerateTask.kt gradle task.
 */
public class DialectGenerate {

  private static final String typeAndName = "\\w+\\s+\\w+";
  private static final String splitDelims = "(\\s|\n|\"|//|/\\*|\\*/|'|\\}|\\{)";

  // Used to split up a string into tokens by the specified deliminators
  // while also keeping the deliminators as tokens.
  private static final Pattern tokenizerPattern = Pattern.compile("((?<="
      + splitDelims + ")|(?=" + splitDelims + "))");

  // Matches function declarations: <return_type> <name> (<args>) :
  private static final Pattern declarationPattern =
    Pattern.compile("(" + typeAndName + "\\s*\\(\\s*(" + typeAndName + "\\s*(\\,\\s*"
        + typeAndName + "\\s*)*)?\\)\\s*\\:\n?)");
  private static final Pattern namePattern = Pattern.compile("\\w+");

  public static Queue<String> getTokens(String input) {
    return new LinkedList(Arrays.asList(tokenizerPattern.split(input)));
  }

  /**
   * Extracts the functions from the given file into functionMap. Parses
   * functions of the form:
   * <return_type> <name>(<args>) :
   * {
   *     <properties>
   * }
   * {
   *     <body>
   * }
   *
   * @param fileText The contents of the file to process
   * @param functionMap The map to which the parsing functions will be added to
   */
  public void processFile(String fileText, Map<String, String> functionMap) {
    // For windows line endings.
    fileText = fileText.replaceAll("\\r\\n", "\n");
    fileText = fileText.replaceAll("\\r", "\n");
    Queue<MatchResult> declarations = new LinkedList();
    Matcher matcher = declarationPattern.matcher(fileText);
    while (matcher.find()) {
      declarations.add(matcher.toMatchResult());
    }
    Queue<String> tokens = getTokens(fileText);
    int charIndex = 0;
    while (!declarations.isEmpty()) {
      MatchResult declaration = declarations.poll();
      while (charIndex < declaration.start()) {
        charIndex += tokens.poll().length();
      }
      charIndex = processFunction(tokens, functionMap, charIndex,
          declaration.end(),  getFunctionName(declaration.group()));
    }
  }

  /**
   * Parses a function of the form:
   *
   * <return_type> <name>(<args>) :
   * {
   *     <properties>
   * }
   * {
   *     <body>
   * }
   *
   * @param tokens The tokens starting from the function declaration and
   *               ending at EOF
   * @param functionMap The map to which the extracted functions will be added to
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   * @param declarationEnd The char index (of entire file text) at which the
   *                       function declaration ends
   * @param functionName The name of the function being processed
   */
  public int processFunction(
      Queue<String> tokens,
      Map<String, String> functionMap,
      int charIndex,
      int declarationEnd,
      String functionName) {
    StringBuilder functionBuilder = new StringBuilder();
    // Process the declaration:
    while (charIndex < declarationEnd && !tokens.isEmpty()) {
      String token = tokens.poll();
      functionBuilder.append(token);
      charIndex += token.length();
    }
    // Called twice as there are two curly blocks, one for initialization
    // and one for the body.
    charIndex = processCurlyBlock(functionBuilder, tokens, charIndex);
    charIndex = processCurlyBlock(functionBuilder, tokens, charIndex);

    functionMap.put(functionName, functionBuilder.toString());
    return charIndex;
  }

  /**
   * Parses a block of text surrounded by curly braces. Creates an instance of
   * CurlyParser() that is used keep track of whether or not the curly block
   * has been fully parsed or not. The current character index of the file text
   * is updated and returned once parsing is complete.
   *
   * @param functionBuilder The builder unto which the tokens get added to once parsed
   * @param tokens The tokens starting from the function declaration and
   *               ending at EOF
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   */
  private int processCurlyBlock(
      StringBuilder functionBuilder,
      Queue<String> tokens,
      int charIndex) throws IllegalStateException {
    // Remove any preceeding spaces or new lines before the curly block starts.
    charIndex = consumeExtraSpacesAndLines(functionBuilder, tokens, charIndex);
    if (tokens.isEmpty() || !tokens.peek().equals("{")) {
      throw new IllegalStateException("First token of curly block must be a curly brace.");
    }
    CurlyParser parser = new CurlyParser();
    while (!tokens.isEmpty()) {
      String token = tokens.poll();
      functionBuilder.append(token);
      charIndex += token.length();
      if (parser.parseToken(token)) {
        return charIndex;
      }
    }
    return charIndex;
  }

  /**
   * Consumes tokens while they are either a space or line break. This needs
   * to be called before a curly block begins being parsed to remove any extra
   * spaces or new lines that may preceed it.
   *
   * @param functionBuilder The builder unto which the tokens get added to once parsed
   * @param tokens The tokens starting from charIndex until EOF
   * @param charIndex The character index of the entire text of the file at which
   *                  the parsing is commencing at
   */
  private int consumeExtraSpacesAndLines(
      StringBuilder functionBuilder,
      Queue<String> tokens,
      int charIndex) {
    while (!tokens.isEmpty() && (tokens.peek().equals(" ")
        || tokens.peek().equals("\n"))) {
      String token = tokens.poll();
      charIndex += token.length();
      functionBuilder.append(token);
    }
    return charIndex;
  }

  /**
   * Gets the function name from the declaration.
   *
   * @param functionDeclaration The function declaration of form
   *                            <return_type> <name> (<args>) :
   */
  private String getFunctionName(String functionDeclaration) {
    Matcher m = namePattern.matcher(functionDeclaration);
    // Name is the second match.
    m.find();
    m.find();
    return m.group();
  }
}
